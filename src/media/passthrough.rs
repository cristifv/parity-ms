//! Passthrough: in a two-party call where both legs use the same codec, a leg's packets go to
//! the other leg as they came in, with no decoding. That works for codecs this server can't
//! decode too. The only changes:
//! - **Renumbering:** the payload type, when the legs number the codec or `telephone-event`
//!   differently.
//! - **Dropping:** RFC 4733 events, when the receiving leg didn't negotiate them.
//! - **Soft mute:** while the sending leg is muted, each packet goes out with its audio
//!   replaced by silence ([`silence`]), keeping its sequence number and timestamp.

use std::sync::OnceLock;

use super::LegCodec;
use crate::codec::{Codec, Encoder, FRAME_SAMPLES};
use crate::rtp;

#[derive(Debug, Default, Clone)]
pub struct Passthrough {
    /// (input, output) payload types for the audio and for `telephone-event`.
    rewrites: Vec<(u8, u8)>,
    /// The sender's `telephone-event` payload type, when the receiver has none.
    drop: Option<u8>,
}

impl Passthrough {
    pub fn new(input: Option<&LegCodec>, output: Option<&LegCodec>) -> Passthrough {
        let mut p = Passthrough::default();
        if let (Some(i), Some(o)) = (input, output)
            && i.same_codec(o)
        {
            p.rewrites.push((i.pt, o.pt));
            match (i.dtmf, o.dtmf) {
                (Some(a), Some(b)) => p.rewrites.push((a, b)),
                (Some(a), None) => p.drop = Some(a),
                _ => {}
            }
            p.rewrites.retain(|(from, to)| from != to);
        }
        p
    }

    /// Prepares a packet for the other leg; `false` means don't send it.
    pub fn apply(&self, packet: &mut [u8], header: &rtp::Header) -> bool {
        if self.drop == Some(header.payload_type) {
            return false;
        }
        if let Some(&(_, to)) = self.rewrites.iter().find(|(from, _)| *from == header.payload_type) {
            rtp::set_payload_type(packet, to);
        }
        true
    }
}

/// Soft mute: writes into `out` the packet with its payload replaced by silence in its
/// codec, keeping the header (sequence number, timestamp, SSRC, payload type).
/// - **G.711:** every byte becomes the silence code, so the payload keeps its length.
/// - **Opus:** a 20 ms silence frame.
///
/// Returns false for a codec we can't make silence in: the packet is dropped instead
/// (a hard mute for that leg).
pub fn silence(packet: &[u8], header: &rtp::Header, codec: Option<Codec>, out: &mut Vec<u8>) -> bool {
    out.clear();
    out.extend_from_slice(&packet[..header.payload.start]);
    // Any padding is gone with the old payload.
    out[0] &= !0x20;
    let len = header.payload.len();
    match codec {
        Some(Codec::Pcmu) => out.resize(out.len() + len, 0xFF),
        Some(Codec::Pcma) => out.resize(out.len() + len, 0xD5),
        Some(Codec::Opus) => match opus_silence() {
            Some(frame) => out.extend_from_slice(frame),
            None => return false,
        },
        #[cfg(feature = "test-codec")]
        Some(Codec::Pulse) => out.push(0),
        None => return false,
    }
    true
}

/// One 20 ms Opus frame of silence, encoded once.
fn opus_silence() -> Option<&'static [u8]> {
    static FRAME: OnceLock<Option<Vec<u8>>> = OnceLock::new();
    FRAME
        .get_or_init(|| {
            let mut encoder = Encoder::new(Codec::Opus).ok()?;
            let mut payload = Vec::new();
            encoder.encode(&[0; FRAME_SAMPLES], &mut payload).ok()?;
            Some(payload)
        })
        .as_deref()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leg(name: &str, pt: u8, dtmf: Option<u8>) -> LegCodec {
        let supported = Codec::from_rtpmap(name, 48000);
        LegCodec { codec: name.into(), pt, dtmf, clock: 48000, supported }
    }

    fn pt_after(p: &Passthrough, pt: u8) -> Option<u8> {
        let mut packet = Vec::new();
        rtp::write(&mut packet, false, pt, 1, 2, 3, b"x");
        let header = rtp::parse(&packet).unwrap();
        p.apply(&mut packet, &header).then(|| rtp::parse(&packet).unwrap().payload_type)
    }

    #[test]
    fn renumbers_only_the_negotiated_payload_types() {
        let p = Passthrough::new(Some(&leg("opus", 111, Some(110))), Some(&leg("opus", 96, Some(97))));
        assert_eq!(pt_after(&p, 111), Some(96));
        assert_eq!(pt_after(&p, 110), Some(97), "telephone-event too");
        assert_eq!(pt_after(&p, 13), Some(13), "anything else is left alone");
    }

    #[test]
    fn soft_mute_keeps_the_header_and_silences_the_audio() {
        let mut packet = Vec::new();
        rtp::write(&mut packet, true, 0, 77, 16_000, 9, &[0x12; 160]);
        let header = rtp::parse(&packet).unwrap();
        let mut out = Vec::new();
        assert!(silence(&packet, &header, Some(Codec::Pcmu), &mut out));
        let h = rtp::parse(&out).unwrap();
        assert_eq!((h.marker, h.payload_type, h.sequence, h.timestamp, h.ssrc), (true, 0, 77, 16_000, 9));
        assert_eq!(out[h.payload.clone()], [0xFF; 160], "μ-law silence, same length");

        assert!(silence(&packet, &header, Some(Codec::Pcma), &mut out));
        assert!(out[header.payload.clone()].iter().all(|&b| b == 0xD5), "A-law silence");

        assert!(silence(&packet, &header, Some(Codec::Opus), &mut out));
        let mut pcm = Vec::new();
        Codec::Opus.decoder().unwrap().decode(&out[header.payload.start..], &mut pcm).unwrap();
        assert!(pcm.len() == FRAME_SAMPLES && pcm.iter().all(|&s| s.abs() < 50), "Opus decodes to silence");

        assert!(!silence(&packet, &header, None, &mut out), "G.729 and the like: dropped");
    }

    #[test]
    fn drops_events_the_receiver_did_not_negotiate() {
        let p = Passthrough::new(Some(&leg("g729", 18, Some(101))), Some(&leg("g729", 18, None)));
        assert_eq!(pt_after(&p, 18), Some(18), "a codec we can't decode still passes");
        assert_eq!(pt_after(&p, 101), None);
    }
}
