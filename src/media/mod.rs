//! Media building blocks used by the leg threads.
//! - **Passthrough:** forward a peer's packets unchanged.
//! - **Transcoding:** decoded audio, mixed and re-encoded into a leg's own stream.
//! - **Mixing:** the shared frames each leg publishes for the others to hear.
//! - **Prompts:** audio files played into a leg.

pub mod mixer;
pub mod passthrough;
pub mod prompt;
pub mod rtcp;
pub mod transcode;

use serde::{Deserialize, Serialize};

use crate::codec::Codec;
use crate::sdp::Payload;

/// What a leg negotiated: its codec and payload type, and the payload type of its RFC 4733
/// `telephone-event` stream (same clock rate), if any.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LegCodec {
    /// Encoding name in lower case: `pcmu`, `pcma`, `opus`, or one we only pass through
    /// (e.g. `g729`).
    pub codec: String,
    pub pt: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dtmf: Option<u8>,
    #[serde(skip)]
    pub clock: u32,
    /// Set when this server can decode and encode the codec.
    #[serde(skip)]
    pub supported: Option<Codec>,
}

impl LegCodec {
    pub fn from_payload(p: &Payload, dtmf: Option<u8>) -> LegCodec {
        LegCodec { codec: p.name.to_ascii_lowercase(), pt: p.pt, dtmf, clock: p.clock_rate, supported: p.codec }
    }

    /// The same encoding, whatever payload type each side uses for it.
    pub fn same_codec(&self, other: &LegCodec) -> bool {
        self.codec == other.codec && self.clock == other.clock
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Passthrough,
    Transcode,
}

/// A leg of a two-party call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    A,
    B,
}

impl Side {
    pub fn other(self) -> Side {
        match self {
            Side::A => Side::B,
            Side::B => Side::A,
        }
    }
}
