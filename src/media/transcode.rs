//! Transcoding, in two halves that meet at 8 kHz PCM:
//! - [`Source`]: the codec a leg sends us. Its [`Decoder`] (one enum variant per codec)
//!   turns that leg's RTP payloads into 20 ms frames of PCM.
//! - [`Destination`]: the codec a leg receives. Its [`Encoder`] (one enum variant per codec)
//!   turns PCM frames (the mix of the other participants) into that leg's own RTP stream,
//!   with its own SSRC, sequence numbers and timestamps. RFC 4733 event packets go in the
//!   same stream.
//!
//! Each leg's inbound side owns a `Source` and, when its stream is made here, its outbound
//! side owns a `Destination`. A call between PCMU and Opus is then two of each: PCMU source
//! to Opus destination one way, Opus source to PCMU destination the other.

use std::hash::{BuildHasher, Hasher};

use super::mixer::Frame;
use super::rtcp;
use crate::clock::unix_ms;
use crate::codec::{Codec, CodecError, Decoder, Encoder, FRAME_SAMPLES, PCM_RATE};
use crate::dtmf::rfc4733;
use crate::rtp;

/// RTP clock ticks covered by `samples` of 8 kHz PCM, in `codec`'s clock.
fn ticks(codec: Codec, samples: usize) -> u32 {
    (samples as u64 * codec.clock_rate() as u64 / PCM_RATE as u64) as u32
}

/// The codec a leg sends us, and its decoder state.
pub struct Source {
    pt: u8,
    decoder: Decoder,
}

impl Source {
    pub fn new(codec: Codec, pt: u8) -> Result<Source, CodecError> {
        Ok(Source { pt, decoder: Decoder::new(codec)? })
    }

    pub fn codec(&self) -> Codec {
        self.decoder.codec()
    }

    pub fn pt(&self) -> u8 {
        self.pt
    }

    /// Decodes one RTP payload, appending PCM to `pcm`. A payload that doesn't decode, or
    /// decodes to nothing, is replaced by one concealed frame.
    pub fn decode(&mut self, payload: &[u8], pcm: &mut Vec<i16>) {
        let before = pcm.len();
        if let Err(e) = self.decoder.decode(payload, pcm) {
            tracing::debug!(error = %e, "decode failed, frame concealed");
        }
        if pcm.len() == before {
            self.conceal(pcm);
        }
    }

    /// Appends one frame in place of a lost packet.
    pub fn conceal(&mut self, pcm: &mut Vec<i16>) {
        self.decoder.conceal(FRAME_SAMPLES, pcm);
    }

    /// RTP clock ticks covered by `samples` of decoded PCM.
    pub fn ticks(&self, samples: usize) -> u32 {
        ticks(self.codec(), samples)
    }
}

/// The codec a leg receives, its encoder state, and the RTP stream we send it.
pub struct Destination {
    pt: u8,
    encoder: Encoder,
    ssrc: u32,
    sequence: u16,
    /// Timestamp of the next frame.
    timestamp: u32,
    marker: bool,
    payload: Vec<u8>,
    /// Sent so far, for RTCP Sender Reports.
    packets: u32,
    octets: u32,
}

impl Destination {
    pub fn new(codec: Codec, pt: u8) -> Result<Destination, CodecError> {
        Ok(Destination {
            pt,
            encoder: Encoder::new(codec)?,
            ssrc: random_u32(),
            sequence: random_u32() as u16,
            timestamp: random_u32(),
            marker: true,
            payload: Vec::new(),
            packets: 0,
            octets: 0,
        })
    }

    pub fn codec(&self) -> Codec {
        self.encoder.codec()
    }

    pub fn pt(&self) -> u8 {
        self.pt
    }

    /// Ticks of this stream's RTP clock per 20 ms frame (160 for G.711, 960 for Opus).
    pub fn frame_ticks(&self) -> u32 {
        ticks(self.codec(), FRAME_SAMPLES)
    }

    pub fn position(&self) -> u32 {
        self.timestamp
    }

    /// One 20 ms packet. `None` if the encoder failed (the frame is skipped).
    pub fn frame(&mut self, pcm: &Frame) -> Option<Vec<u8>> {
        let packet = match self.encoder.encode(pcm, &mut self.payload) {
            Ok(()) => {
                let mut p = Vec::with_capacity(rtp::HEADER_LEN + self.payload.len());
                rtp::write(&mut p, self.marker, self.pt, self.sequence, self.timestamp, self.ssrc, &self.payload);
                self.sequence = self.sequence.wrapping_add(1);
                self.marker = false;
                self.count(self.payload.len());
                Some(p)
            }
            Err(e) => {
                tracing::debug!(error = %e, "encode failed, frame skipped");
                None
            }
        };
        self.skip_frame();
        packet
    }

    /// Moves the clock on by one frame without sending audio (e.g. during a DTMF event).
    pub fn skip_frame(&mut self) {
        self.timestamp = self.timestamp.wrapping_add(self.frame_ticks());
    }

    /// Private API: moves the RTP clock past `frames` frames that were never sent (the RTP
    /// thread missed them). The receiver sees a gap in the timestamps and plays it as
    /// silence or concealment. The next packet carries the marker bit.
    pub(crate) fn skip_frames(&mut self, frames: u32) {
        self.timestamp = self.timestamp.wrapping_add(self.frame_ticks().wrapping_mul(frames));
        self.marker = true;
    }

    /// The next audio packet starts a new talkspurt (marker bit).
    pub fn mark(&mut self) {
        self.marker = true;
    }

    /// An RFC 4733 event packet in this stream.
    pub fn event_packet(&mut self, pt: u8, event: rfc4733::Event, marker: bool, timestamp: u32) -> Vec<u8> {
        let mut packet = Vec::new();
        rtp::write(&mut packet, marker, pt, self.sequence, timestamp, self.ssrc, &event.to_bytes());
        self.sequence = self.sequence.wrapping_add(1);
        self.count(rfc4733::PAYLOAD_LEN);
        packet
    }

    fn count(&mut self, payload_bytes: usize) {
        self.packets = self.packets.wrapping_add(1);
        self.octets = self.octets.wrapping_add(payload_bytes as u32);
    }

    pub fn packets_sent(&self) -> u32 {
        self.packets
    }

    /// An RTCP Sender Report tying this stream's RTP clock to wall-clock time now. The next
    /// frame is due now, so its timestamp is the one that matches.
    pub fn sender_report(&self, cname: &str) -> Vec<u8> {
        let info = rtcp::SenderInfo {
            ssrc: self.ssrc,
            at_ms: unix_ms(),
            rtp_timestamp: self.timestamp,
            packets: self.packets,
            octets: self.octets,
        };
        rtcp::sender_report(&info, cname)
    }
}

fn random_u32() -> u32 {
    std::collections::hash_map::RandomState::new().build_hasher().finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_source_decodes_what_a_destination_encodes() {
        let tone: Frame = std::array::from_fn(|i| ((i as f32 * 0.3).sin() * 8000.0) as i16);
        for (codec, pt) in [(Codec::Pcmu, 0), (Codec::Pcma, 8), (Codec::Opus, 111)] {
            let mut destination = Destination::new(codec, pt).unwrap();
            let mut source = Source::new(codec, pt).unwrap();
            assert_eq!((source.codec(), source.pt()), (destination.codec(), destination.pt()));

            let mut pcm = Vec::new();
            for _ in 0..5 {
                let packet = destination.frame(&tone).unwrap();
                let header = rtp::parse(&packet).unwrap();
                assert_eq!(header.payload_type, pt);
                pcm.clear();
                source.decode(&packet[header.payload], &mut pcm);
                assert_eq!(pcm.len(), FRAME_SAMPLES, "{codec:?}");
                assert_eq!(source.ticks(pcm.len()), destination.frame_ticks());
            }
            if codec != Codec::Opus {
                let worst = pcm.iter().zip(&tone).map(|(a, b)| (*a as i32 - *b as i32).abs()).max();
                assert!(worst.unwrap() < 600, "{codec:?}: G.711 keeps the waveform");
            }
        }
    }

    #[test]
    fn a_payload_that_does_not_decode_is_concealed() {
        for codec in Codec::ALL {
            let mut source = Source::new(codec, 96).unwrap();
            let mut pcm = Vec::new();
            source.decode(&[], &mut pcm);
            assert_eq!(pcm.len(), FRAME_SAMPLES, "{codec:?}: one frame in its place");
        }
    }

    #[test]
    fn frames_and_events_share_one_stream() {
        let mut s = Destination::new(Codec::Opus, 111).unwrap();
        let first = rtp::parse(&s.frame(&[100; FRAME_SAMPLES]).unwrap()).unwrap();
        assert!(first.marker);
        assert_eq!(first.payload_type, 111);

        let at = s.position();
        let ev = rfc4733::Event { event: 3, end: false, volume: 10, duration: 960 };
        let event = rtp::parse(&s.event_packet(101, ev, true, at)).unwrap();
        s.skip_frame();
        s.mark();
        let next = rtp::parse(&s.frame(&[0; FRAME_SAMPLES]).unwrap()).unwrap();

        assert_eq!(event.timestamp.wrapping_sub(first.timestamp), 960, "Opus: 960 ticks per 20 ms");
        assert_eq!(next.timestamp.wrapping_sub(event.timestamp), 960);
        assert_eq!(event.sequence, first.sequence.wrapping_add(1));
        assert_eq!(next.sequence, event.sequence.wrapping_add(1));
        assert!(next.marker && event.marker);
        assert!(event.ssrc == first.ssrc && next.ssrc == first.ssrc);
    }

    #[test]
    fn g711_frames_are_160_bytes_and_160_ticks() {
        let mut s = Destination::new(Codec::Pcma, 8).unwrap();
        let a = s.frame(&[0; FRAME_SAMPLES]).unwrap();
        let b = s.frame(&[0; FRAME_SAMPLES]).unwrap();
        assert_eq!(a.len(), rtp::HEADER_LEN + 160);
        let (ha, hb) = (rtp::parse(&a).unwrap(), rtp::parse(&b).unwrap());
        assert_eq!(hb.timestamp.wrapping_sub(ha.timestamp), 160);
        assert!(!hb.marker);
    }

    #[test]
    fn skipped_frames_move_the_clock_and_mark_the_next_packet() {
        let mut d = Destination::new(Codec::Opus, 111).unwrap();
        let first = rtp::parse(&d.frame(&[0; FRAME_SAMPLES]).unwrap()).unwrap();
        d.skip_frames(10);
        let next = rtp::parse(&d.frame(&[0; FRAME_SAMPLES]).unwrap()).unwrap();
        assert_eq!(next.timestamp.wrapping_sub(first.timestamp), 11 * 960, "this frame, then 10 missed");
        assert_eq!(next.sequence, first.sequence.wrapping_add(1), "nothing was sent in between");
        assert!(next.marker);
    }

    #[test]
    fn sender_reports_count_what_was_sent_and_carry_the_clock() {
        let mut s = Destination::new(Codec::Pcmu, 0).unwrap();
        for _ in 0..3 {
            s.frame(&[0; FRAME_SAMPLES]).unwrap();
        }
        let sr = rtcp::parse_sender_report(&s.sender_report("test")).unwrap();
        assert_eq!((sr.packets, sr.octets), (3, 480));
        assert_eq!(sr.rtp_timestamp, s.position(), "the timestamp of the frame due now");
        let ntp_unix_ms = ((sr.ntp >> 32) - 2_208_988_800) * 1000;
        assert!(unix_ms().abs_diff(ntp_unix_ms) < 2000);
    }
}
