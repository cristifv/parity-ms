//! Audio files to play into a leg (`play`). A file is loaded and decoded on the main thread,
//! then handed to the leg's RTP thread as 8 kHz PCM, so the RTP thread never touches the
//! disk.
//!
//! Files come from the prompt directory (`--prompt-dir`) only: a name is a relative path
//! inside it, and anything that would leave it (`..`, an absolute path, a symlink pointing
//! out) is refused.
//!
//! Formats: WAV or Sun `.au`, mono, 8 kHz, as 16-bit linear PCM, μ-law or A-law.

use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crate::codec::{FRAME_SAMPLES, PCM_RATE, alaw_to_linear, ulaw_to_linear};

/// Largest file accepted: about 33 minutes of 16-bit audio.
const MAX_FILE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum PromptError {
    #[error("{0}")]
    BadName(String),
    #[error("{0} not found")]
    NotFound(String),
    #[error("{name}: {message}")]
    Format { name: String, message: String },
    #[error("{name}: {error}")]
    Io { name: String, error: io::Error },
}

/// A decoded audio file.
#[derive(Debug)]
pub struct Prompt {
    /// The name it was asked for by.
    pub name: String,
    pub pcm: Arc<[i16]>,
}

impl Prompt {
    pub fn duration_ms(&self) -> u64 {
        self.pcm.len() as u64 * 1000 / PCM_RATE as u64
    }

    /// Frames it takes to play, the last one padded with silence.
    pub fn frames(&self) -> usize {
        self.pcm.len().div_ceil(FRAME_SAMPLES)
    }
}

/// Loads `name` from `dir`.
pub fn load(dir: &Path, name: &str) -> Result<Prompt, PromptError> {
    let path = resolve(dir, name)?;
    let meta = std::fs::metadata(&path).map_err(|e| io_error(name, e))?;
    if meta.len() > MAX_FILE_BYTES {
        return Err(format_error(name, format!("larger than {MAX_FILE_BYTES} bytes")));
    }
    let bytes = std::fs::read(&path).map_err(|e| io_error(name, e))?;
    let pcm = decode(&bytes).map_err(|m| format_error(name, m))?;
    Ok(Prompt { name: name.to_string(), pcm: pcm.into() })
}

fn io_error(name: &str, error: io::Error) -> PromptError {
    match error.kind() {
        io::ErrorKind::NotFound => PromptError::NotFound(name.to_string()),
        _ => PromptError::Io { name: name.to_string(), error },
    }
}

fn format_error(name: &str, message: impl Into<String>) -> PromptError {
    PromptError::Format { name: name.to_string(), message: message.into() }
}

/// The file `name` names inside `dir`, refusing anything that leads out of it.
fn resolve(dir: &Path, name: &str) -> Result<PathBuf, PromptError> {
    let bad = |why: &str| Err(PromptError::BadName(format!("file {name:?}: {why}")));
    if name.is_empty() || name.contains('\0') {
        return bad("empty or invalid");
    }
    let relative = Path::new(name);
    if !relative.components().all(|c| matches!(c, Component::Normal(_))) {
        return bad("must be a path inside the prompt directory, without `..`");
    }
    let path = dir.join(relative);
    // A symlink inside the directory must not point out of it.
    if let (Ok(real), Ok(root)) = (path.canonicalize(), dir.canonicalize())
        && !real.starts_with(&root)
    {
        return bad("leads out of the prompt directory");
    }
    Ok(path)
}

#[derive(Clone, Copy)]
enum Encoding {
    Linear16 { big_endian: bool },
    Ulaw,
    Alaw,
}

/// Decodes a WAV or `.au` file into 8 kHz mono PCM.
fn decode(bytes: &[u8]) -> Result<Vec<i16>, String> {
    let (encoding, rate, channels, data) = if bytes.starts_with(b".snd") {
        parse_au(bytes)?
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WAVE") {
        parse_wav(bytes)?
    } else {
        return Err("not a WAV or .au file".into());
    };
    if rate != PCM_RATE {
        return Err(format!("{rate} Hz; only {PCM_RATE} Hz is played"));
    }
    if channels != 1 {
        return Err(format!("{channels} channels; only mono is played"));
    }
    Ok(match encoding {
        Encoding::Linear16 { big_endian } => data
            .chunks_exact(2)
            .map(|b| if big_endian { i16::from_be_bytes([b[0], b[1]]) } else { i16::from_le_bytes([b[0], b[1]]) })
            .collect(),
        Encoding::Ulaw => data.iter().map(|&b| ulaw_to_linear(b)).collect(),
        Encoding::Alaw => data.iter().map(|&b| alaw_to_linear(b)).collect(),
    })
}

fn be32(b: &[u8], at: usize) -> Result<u32, String> {
    b.get(at..at + 4).map(|s| u32::from_be_bytes(s.try_into().unwrap())).ok_or_else(|| "truncated header".into())
}

fn le32(b: &[u8], at: usize) -> Result<u32, String> {
    b.get(at..at + 4).map(|s| u32::from_le_bytes(s.try_into().unwrap())).ok_or_else(|| "truncated header".into())
}

fn le16(b: &[u8], at: usize) -> Result<u16, String> {
    b.get(at..at + 2).map(|s| u16::from_le_bytes(s.try_into().unwrap())).ok_or_else(|| "truncated header".into())
}

fn parse_au(b: &[u8]) -> Result<(Encoding, u32, u32, &[u8]), String> {
    let offset = be32(b, 4)? as usize;
    let size = be32(b, 8)?;
    let encoding = match be32(b, 12)? {
        1 => Encoding::Ulaw,
        3 => Encoding::Linear16 { big_endian: true },
        27 => Encoding::Alaw,
        e => return Err(format!(".au encoding {e}; only 16-bit linear, μ-law and A-law are played")),
    };
    let (rate, channels) = (be32(b, 16)?, be32(b, 20)?);
    let data = b.get(offset..).ok_or("data offset past the end")?;
    // 0xFFFFFFFF: size unknown, the data runs to the end.
    let data = if size == u32::MAX { data } else { &data[..data.len().min(size as usize)] };
    Ok((encoding, rate, channels, data))
}

fn parse_wav(b: &[u8]) -> Result<(Encoding, u32, u32, &[u8]), String> {
    let mut at = 12;
    let mut format = None;
    while at + 8 <= b.len() {
        let (id, len) = (&b[at..at + 4], le32(b, at + 4)? as usize);
        let body = at + 8;
        match id {
            b"fmt " => {
                let encoding = match (le16(b, body)?, le16(b, body + 14)?) {
                    (1, 16) => Encoding::Linear16 { big_endian: false },
                    (6, 8) => Encoding::Alaw,
                    (7, 8) => Encoding::Ulaw,
                    (tag, bits) => {
                        return Err(format!("WAV format {tag} at {bits} bits; only 16-bit PCM, μ-law and A-law are played"));
                    }
                };
                format = Some((encoding, le32(b, body + 4)?, le16(b, body + 2)? as u32));
            }
            b"data" => {
                let (encoding, rate, channels) = format.ok_or("WAV data before its fmt chunk")?;
                let data = &b[body..b.len().min(body + len)];
                return Ok((encoding, rate, channels, data));
            }
            _ => {}
        }
        // Chunks are padded to an even length.
        at = body + len + (len & 1);
    }
    Err("WAV without a data chunk".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::linear_to_ulaw;

    fn wav(samples: &[i16]) -> Vec<u8> {
        let data: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
        let mut w = Vec::new();
        w.extend_from_slice(b"RIFF");
        w.extend_from_slice(&(36 + data.len() as u32).to_le_bytes());
        w.extend_from_slice(b"WAVEfmt ");
        w.extend_from_slice(&16u32.to_le_bytes());
        w.extend_from_slice(&1u16.to_le_bytes()); // PCM
        w.extend_from_slice(&1u16.to_le_bytes()); // mono
        w.extend_from_slice(&8000u32.to_le_bytes());
        w.extend_from_slice(&16000u32.to_le_bytes());
        w.extend_from_slice(&2u16.to_le_bytes());
        w.extend_from_slice(&16u16.to_le_bytes());
        w.extend_from_slice(b"LIST\x03\x00\x00\x00abc\x00"); // an odd-sized chunk to skip
        w.extend_from_slice(b"data");
        w.extend_from_slice(&(data.len() as u32).to_le_bytes());
        w.extend_from_slice(&data);
        w
    }

    #[test]
    fn wav_and_au_decode_to_8khz_pcm() {
        let samples = [0, 1000, -1000, 32767, -32768];
        assert_eq!(decode(&wav(&samples)).unwrap(), samples);

        let mut au = Vec::new();
        // magic, data offset, data size, encoding (1: μ-law), rate, channels
        for v in [0x2e73_6e64u32, 24, 5, 1, 8000, 1] {
            au.extend_from_slice(&v.to_be_bytes());
        }
        au.extend(samples.iter().map(|&s| linear_to_ulaw(s)));
        let decoded = decode(&au).unwrap();
        assert_eq!(decoded.len(), 5);
        assert!((decoded[1] - 1000).abs() < 64, "μ-law: {decoded:?}");
    }

    #[test]
    fn other_rates_channels_and_formats_are_refused() {
        let mut w = wav(&[0; 10]);
        w[24..28].copy_from_slice(&16000u32.to_le_bytes());
        assert!(decode(&w).unwrap_err().contains("16000 Hz"));
        let mut w = wav(&[0; 10]);
        w[22..24].copy_from_slice(&2u16.to_le_bytes());
        assert!(decode(&w).unwrap_err().contains("2 channels"));
        assert!(decode(b"ID3 not audio").is_err());
    }

    /// A fresh directory for one test.
    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("media-server-prompt-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn names_must_stay_inside_the_prompt_directory() {
        let dir = scratch("prompts");
        std::fs::create_dir(dir.join("en")).unwrap();
        std::fs::write(dir.join("en/hello.wav"), wav(&[5; 400])).unwrap();
        let p = load(&dir, "en/hello.wav").unwrap();
        assert_eq!((p.pcm.len(), p.duration_ms(), p.frames()), (400, 50, 3));

        for name in ["../etc/passwd", "/etc/passwd", "en/../../x.wav", "", "./en/hello.wav"] {
            assert!(matches!(load(&dir, name), Err(PromptError::BadName(_))), "{name:?}");
        }
        assert!(matches!(load(&dir, "missing.wav"), Err(PromptError::NotFound(_))));

        #[cfg(unix)]
        {
            let outside = scratch("outside");
            std::fs::write(outside.join("secret.wav"), wav(&[1; 10])).unwrap();
            std::os::unix::fs::symlink(outside.join("secret.wav"), dir.join("link.wav")).unwrap();
            assert!(matches!(load(&dir, "link.wav"), Err(PromptError::BadName(_))));
            std::fs::remove_dir_all(outside).unwrap();
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
