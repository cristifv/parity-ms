//! Audio codecs. Everything decodes to, and encodes from, 8 kHz mono 16-bit PCM, which
//! G.711 uses natively and libopus can encode and decode directly, so nothing resamples.
//!
//! - [`Codec`] names a codec (for SDP and configuration).
//! - [`Decoder`] and [`Encoder`] are enums with one variant per codec, holding that codec's
//!   state. G.711 keeps none; Opus holds its libopus decoder or encoder.

mod g711;
mod opus;
#[cfg(feature = "test-codec")]
pub mod pulse;

use serde::Serialize;

/// Sample rate of the PCM exchanged between codecs, the transcoder and the recorder.
pub const PCM_RATE: u32 = 8000;
/// One 20 ms frame at `PCM_RATE`.
pub const FRAME_SAMPLES: usize = 160;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    Pcmu,
    Pcma,
    Opus,
    /// Pulse positions, for tests only (see [`pulse`]).
    #[cfg(feature = "test-codec")]
    Pulse,
}

impl Codec {
    /// The codecs offered to transcode to. The test codec is never offered: it's used only
    /// when an endpoint offers it.
    pub const ALL: [Codec; 3] = [Codec::Pcmu, Codec::Pcma, Codec::Opus];

    /// Encoding name as written in an SDP `rtpmap`.
    pub fn sdp_name(self) -> &'static str {
        match self {
            Codec::Pcmu => "PCMU",
            Codec::Pcma => "PCMA",
            Codec::Opus => "opus",
            #[cfg(feature = "test-codec")]
            Codec::Pulse => "x-pulse",
        }
    }

    /// RTP clock rate. Opus always uses 48 kHz on the wire (RFC 7587).
    pub fn clock_rate(self) -> u32 {
        match self {
            Codec::Pcmu | Codec::Pcma => 8000,
            Codec::Opus => 48000,
            #[cfg(feature = "test-codec")]
            Codec::Pulse => 8000,
        }
    }

    /// Channel count declared in SDP (`opus/48000/2`, as RFC 7587 requires).
    pub fn sdp_channels(self) -> Option<u8> {
        match self {
            Codec::Opus => Some(2),
            _ => None,
        }
    }

    /// Payload type to offer when this codec is added to an SDP.
    pub fn static_payload_type(self) -> Option<u8> {
        match self {
            Codec::Pcmu => Some(0),
            Codec::Pcma => Some(8),
            _ => None,
        }
    }

    pub fn from_static_payload_type(pt: u8) -> Option<Codec> {
        match pt {
            0 => Some(Codec::Pcmu),
            8 => Some(Codec::Pcma),
            _ => None,
        }
    }

    pub fn from_rtpmap(name: &str, clock_rate: u32) -> Option<Codec> {
        match (name.to_ascii_lowercase().as_str(), clock_rate) {
            ("pcmu", 8000) => Some(Codec::Pcmu),
            ("pcma", 8000) => Some(Codec::Pcma),
            ("opus", 48000) => Some(Codec::Opus),
            #[cfg(feature = "test-codec")]
            ("x-pulse", 8000) => Some(Codec::Pulse),
            _ => None,
        }
    }

    pub fn decoder(self) -> Result<Decoder, CodecError> {
        Decoder::new(self)
    }

    pub fn encoder(self) -> Result<Encoder, CodecError> {
        Encoder::new(self)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{codec:?}: {message}")]
pub struct CodecError {
    pub codec: Codec,
    pub message: String,
}

/// Decodes one codec's RTP payloads into 8 kHz PCM.
pub enum Decoder {
    Pcmu,
    Pcma,
    Opus(opus::Decoder),
    #[cfg(feature = "test-codec")]
    Pulse,
}

impl Decoder {
    pub fn new(codec: Codec) -> Result<Decoder, CodecError> {
        Ok(match codec {
            Codec::Pcmu => Decoder::Pcmu,
            Codec::Pcma => Decoder::Pcma,
            Codec::Opus => Decoder::Opus(opus::Decoder::new()?),
            #[cfg(feature = "test-codec")]
            Codec::Pulse => Decoder::Pulse,
        })
    }

    pub fn codec(&self) -> Codec {
        match self {
            Decoder::Pcmu => Codec::Pcmu,
            Decoder::Pcma => Codec::Pcma,
            Decoder::Opus(_) => Codec::Opus,
            #[cfg(feature = "test-codec")]
            Decoder::Pulse => Codec::Pulse,
        }
    }

    /// Decodes one RTP payload, appending 8 kHz mono PCM to `pcm`.
    pub fn decode(&mut self, payload: &[u8], pcm: &mut Vec<i16>) -> Result<(), CodecError> {
        match self {
            Decoder::Pcmu => pcm.extend(payload.iter().map(|&b| g711::ulaw_to_linear(b))),
            Decoder::Pcma => pcm.extend(payload.iter().map(|&b| g711::alaw_to_linear(b))),
            Decoder::Opus(d) => return d.decode(payload, pcm),
            #[cfg(feature = "test-codec")]
            Decoder::Pulse => match pulse::decode(payload) {
                Some(frame) => pcm.extend_from_slice(&frame),
                None => return Err(CodecError { codec: Codec::Pulse, message: "malformed pulse packet".into() }),
            },
        }
        Ok(())
    }

    /// Appends `samples` of audio in place of a lost packet: Opus's own concealment, silence
    /// for G.711.
    pub fn conceal(&mut self, samples: usize, pcm: &mut Vec<i16>) {
        match self {
            Decoder::Opus(d) => d.conceal(samples, pcm),
            _ => pcm.resize(pcm.len() + samples, 0),
        }
    }
}

/// Encodes 8 kHz PCM into one codec's RTP payloads, 20 ms at a time.
pub enum Encoder {
    Pcmu,
    Pcma,
    Opus(opus::Encoder),
    #[cfg(feature = "test-codec")]
    Pulse,
}

impl Encoder {
    pub fn new(codec: Codec) -> Result<Encoder, CodecError> {
        Ok(match codec {
            Codec::Pcmu => Encoder::Pcmu,
            Codec::Pcma => Encoder::Pcma,
            Codec::Opus => Encoder::Opus(opus::Encoder::new()?),
            #[cfg(feature = "test-codec")]
            Codec::Pulse => Encoder::Pulse,
        })
    }

    pub fn codec(&self) -> Codec {
        match self {
            Encoder::Pcmu => Codec::Pcmu,
            Encoder::Pcma => Codec::Pcma,
            Encoder::Opus(_) => Codec::Opus,
            #[cfg(feature = "test-codec")]
            Encoder::Pulse => Codec::Pulse,
        }
    }

    /// Encodes exactly one frame of `FRAME_SAMPLES`, replacing the contents of `payload`.
    pub fn encode(&mut self, pcm: &[i16], payload: &mut Vec<u8>) -> Result<(), CodecError> {
        if pcm.len() != FRAME_SAMPLES {
            return Err(CodecError { codec: self.codec(), message: format!("expected {FRAME_SAMPLES} samples") });
        }
        payload.clear();
        match self {
            Encoder::Pcmu => payload.extend(pcm.iter().map(|&s| g711::linear_to_ulaw(s))),
            Encoder::Pcma => payload.extend(pcm.iter().map(|&s| g711::linear_to_alaw(s))),
            Encoder::Opus(e) => return e.encode(pcm, payload),
            #[cfg(feature = "test-codec")]
            Encoder::Pulse => pulse::encode(pcm, payload),
        }
        Ok(())
    }
}

pub use g711::{alaw_to_linear, linear_to_alaw, linear_to_ulaw, ulaw_to_linear};
