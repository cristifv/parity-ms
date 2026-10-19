//! Opus through libopus (opusic-sys builds the bundled source and links it statically).
//! Runs at 8 kHz mono: narrowband, which is what the other side of a G.711 call carries.

use std::os::raw::c_int;

use opusic_sys as ffi;

use super::{Codec, CodecError, PCM_RATE};

const BITRATE: i32 = 24_000;
/// The longest Opus packet is 120 ms.
const MAX_DECODED: usize = PCM_RATE as usize * 120 / 1000;
const MAX_PACKET: usize = 1500;

fn error(code: c_int, what: &str) -> CodecError {
    let text = unsafe { std::ffi::CStr::from_ptr(ffi::opus_strerror(code)) };
    CodecError { codec: Codec::Opus, message: format!("{what}: {}", text.to_string_lossy()) }
}

pub struct Encoder {
    st: *mut ffi::OpusEncoder,
}

// The state is owned by one Encoder and only used through `&mut self`.
unsafe impl Send for Encoder {}

impl Encoder {
    pub fn new() -> Result<Encoder, CodecError> {
        let mut err: c_int = 0;
        let st = unsafe { ffi::opus_encoder_create(PCM_RATE as i32, 1, ffi::OPUS_APPLICATION_VOIP, &mut err) };
        if st.is_null() || err != ffi::OPUS_OK {
            return Err(error(err, "encoder create"));
        }
        let enc = Encoder { st };
        unsafe {
            ffi::opus_encoder_ctl(st, ffi::OPUS_SET_BITRATE_REQUEST, BITRATE);
            ffi::opus_encoder_ctl(st, ffi::OPUS_SET_SIGNAL_REQUEST, ffi::OPUS_SIGNAL_VOICE);
        }
        Ok(enc)
    }
}

impl Drop for Encoder {
    fn drop(&mut self) {
        unsafe { ffi::opus_encoder_destroy(self.st) }
    }
}

impl Encoder {
    /// Encodes one frame of `FRAME_SAMPLES`, replacing the contents of `payload`.
    pub fn encode(&mut self, pcm: &[i16], payload: &mut Vec<u8>) -> Result<(), CodecError> {
        payload.resize(MAX_PACKET, 0);
        let n = unsafe {
            ffi::opus_encode(self.st, pcm.as_ptr(), pcm.len() as c_int, payload.as_mut_ptr(), MAX_PACKET as i32)
        };
        if n < 0 {
            return Err(error(n, "encode"));
        }
        payload.truncate(n as usize);
        Ok(())
    }
}

pub struct Decoder {
    st: *mut ffi::OpusDecoder,
    buf: Vec<i16>,
}

unsafe impl Send for Decoder {}

impl Decoder {
    pub fn new() -> Result<Decoder, CodecError> {
        let mut err: c_int = 0;
        // A mono decoder also takes stereo streams and downmixes them.
        let st = unsafe { ffi::opus_decoder_create(PCM_RATE as i32, 1, &mut err) };
        if st.is_null() || err != ffi::OPUS_OK {
            return Err(error(err, "decoder create"));
        }
        Ok(Decoder { st, buf: vec![0; MAX_DECODED] })
    }
}

impl Drop for Decoder {
    fn drop(&mut self) {
        unsafe { ffi::opus_decoder_destroy(self.st) }
    }
}

impl Decoder {
    pub fn decode(&mut self, payload: &[u8], pcm: &mut Vec<i16>) -> Result<(), CodecError> {
        if payload.is_empty() {
            return Ok(());
        }
        let n = unsafe {
            ffi::opus_decode(
                self.st,
                payload.as_ptr(),
                payload.len() as i32,
                self.buf.as_mut_ptr(),
                MAX_DECODED as c_int,
                0,
            )
        };
        if n < 0 {
            return Err(error(n, "decode"));
        }
        pcm.extend_from_slice(&self.buf[..n as usize]);
        Ok(())
    }

    /// Packet-loss concealment for `samples` of missing audio.
    pub fn conceal(&mut self, samples: usize, pcm: &mut Vec<i16>) {
        let samples = samples.min(MAX_DECODED);
        // A null packet asks libopus for packet-loss concealment.
        let n = unsafe { ffi::opus_decode(self.st, std::ptr::null(), 0, self.buf.as_mut_ptr(), samples as c_int, 0) };
        match n {
            n if n > 0 => pcm.extend_from_slice(&self.buf[..n as usize]),
            _ => pcm.resize(pcm.len() + samples, 0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::FRAME_SAMPLES;

    #[test]
    fn a_tone_survives_encode_and_decode() {
        let mut enc = Encoder::new().unwrap();
        let mut dec = Decoder::new().unwrap();
        let (mut packet, mut out) = (Vec::new(), Vec::new());
        for frame in 0..25 {
            let pcm: Vec<i16> = (0..FRAME_SAMPLES)
                .map(|i| {
                    let t = (frame * FRAME_SAMPLES + i) as f64 / PCM_RATE as f64;
                    ((t * 440.0 * std::f64::consts::TAU).sin() * 8000.0) as i16
                })
                .collect();
            enc.encode(&pcm, &mut packet).unwrap();
            assert!(!packet.is_empty() && packet.len() < 200, "packet of {} bytes", packet.len());
            dec.decode(&packet, &mut out).unwrap();
        }
        assert_eq!(out.len(), 25 * FRAME_SAMPLES);
        let tail = &out[out.len() - FRAME_SAMPLES * 5..];
        let rms = (tail.iter().map(|&s| (s as f64).powi(2)).sum::<f64>() / tail.len() as f64).sqrt();
        assert!(rms > 3000.0, "decoded tone is too quiet: rms {rms}");
    }
}
