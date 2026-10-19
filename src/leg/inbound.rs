//! The receiving side of a leg: what the endpoint sends us.
//! - **Audio:** packets go through the jitter buffer and are decoded by the leg's transcoding
//!   [`Source`] into 20 ms frames, which are published for the peers to mix.
//! - **DTMF:** RFC 4733 events, or tones found in the decoded audio, become reported digits
//!   (for the `dtmf` event and the recording timeline). In a call they are also signalled to
//!   the peer whose stream is made here.

use std::collections::VecDeque;
use std::time::Instant;

use super::{LegConfig, LegCmd, PeerDtmf, Place};
use crate::codec::{Codec, FRAME_SAMPLES};
use crate::dtmf::inband::{Detection, Detector};
use crate::dtmf::rfc4733::{self, Receiver, Update};
use crate::dtmf::{self, Digit, event_to_char};
use crate::jitter::{JitterBuffer, JitterConfig, JitterStats, Playout};
use crate::media::mixer::{Frame, SILENCE};
use crate::media::transcode::Source;
use crate::rtp;

/// In-band digits end after this many ticks without audio (200 ms).
const AUDIO_GAP_TICKS: u32 = 10;

pub struct Inbound {
    config: JitterConfig,
    jitter: Option<JitterBuffer>,
    /// The codec the leg sends, set by its first decodable packet.
    source: Option<Source>,
    ssrc: Option<u32>,
    pcm: VecDeque<i16>,
    scratch: Vec<i16>,
    receiver: Receiver,
    detector: Detector,
    quiet_ticks: u32,
    updates: Vec<Update>,
    detections: Vec<Detection>,
    /// Digits the leg finished sending, waiting to be taken.
    reported: Vec<Digit>,
}

impl Inbound {
    pub fn new(config: JitterConfig) -> Inbound {
        Inbound {
            config,
            jitter: None,
            source: None,
            ssrc: None,
            pcm: VecDeque::new(),
            scratch: Vec::new(),
            receiver: Receiver::default(),
            detector: Detector::default(),
            quiet_ticks: 0,
            updates: Vec::new(),
            detections: Vec::new(),
            reported: Vec::new(),
        }
    }

    /// Digits reported since the last call.
    pub fn take_digits(&mut self) -> Vec<Digit> {
        std::mem::take(&mut self.reported)
    }

    pub fn stats(&self) -> JitterStats {
        self.jitter.as_ref().map(|j| j.stats()).unwrap_or_default()
    }

    /// The codec of an audio packet, if we can decode it.
    pub(super) fn codec_of(config: &LegConfig, pt: u8) -> Option<Codec> {
        match &config.codec {
            Some(c) if c.pt == pt => c.supported,
            Some(_) => None,
            None => Codec::from_static_payload_type(pt),
        }
    }

    /// An audio packet: queued for decoding, if its codec is one we decode.
    pub fn push_audio(&mut self, config: &LegConfig, header: &rtp::Header, payload: &[u8], now: Instant) {
        let pt = header.payload_type;
        let Some(codec) = Self::codec_of(config, pt) else { return };
        if self.source.as_ref().is_none_or(|s| (s.codec(), s.pt()) != (codec, pt)) {
            // A new codec: a new decoder, and a jitter buffer in its clock.
            match Source::new(codec, pt) {
                Ok(source) => self.source = Some(source),
                Err(e) => {
                    tracing::warn!(error = %e, "can't decode this leg's audio");
                    return;
                }
            }
            self.jitter = Some(JitterBuffer::new(codec.clock_rate(), self.config));
            self.pcm.clear();
            self.ssrc = Some(header.ssrc);
        } else if self.ssrc != Some(header.ssrc) {
            // Same codec, new stream: its sequence numbers and timestamps start over.
            if let Some(j) = &mut self.jitter {
                j.reset();
            }
            self.ssrc = Some(header.ssrc);
        }
        if let Some(j) = &mut self.jitter {
            j.push(header.timestamp, header.sequence, payload, now);
        }
    }

    /// An RFC 4733 packet: reports finished digits and signals the peer.
    pub fn on_event(&mut self, config: &LegConfig, ts: u32, event: rfc4733::Event, now: Instant) {
        let mut updates = std::mem::take(&mut self.updates);
        updates.clear();
        self.receiver.on_packet(ts, event, now, &mut updates);
        let clock = config.codec.as_ref().map_or(8000, |c| c.clock);
        for update in updates.drain(..) {
            self.on_update(config, update, clock);
        }
        self.updates = updates;
    }

    fn on_update(&mut self, config: &LegConfig, update: Update, clock: u32) {
        match update {
            Update::Start { event } => signal(config, PeerDtmf::Start(event), true),
            Update::End { event, duration } => {
                self.report(&config.place, event, duration as u32 * 1000 / clock.max(1), dtmf::Source::Rfc4733);
                signal(config, PeerDtmf::End, true);
            }
            Update::Continue { .. } | Update::Repeat { .. } => {}
        }
    }

    /// One 20 ms tick: the next frame of decoded audio, if the leg is sending any.
    pub fn tick(&mut self, config: &LegConfig, now: Instant) -> Option<Frame> {
        if let Some(update) = self.receiver.expire(now) {
            let clock = config.codec.as_ref().map_or(8000, |c| c.clock);
            self.on_update(config, update, clock);
        }
        let frame = self.next_frame(now);
        // Without RFC 4733, the leg's digits can only be heard in its audio.
        if config.codec.as_ref().is_none_or(|c| c.dtmf.is_none()) {
            self.detect(config, frame.as_ref());
        }
        frame
    }

    fn next_frame(&mut self, now: Instant) -> Option<Frame> {
        let (Some(jitter), Some(source)) = (&mut self.jitter, &mut self.source) else { return None };
        while self.pcm.len() < FRAME_SAMPLES {
            self.scratch.clear();
            match jitter.pop(now) {
                Playout::Packet(p) => {
                    source.decode(&p.payload, &mut self.scratch);
                    jitter.played(p.timestamp, source.ticks(self.scratch.len()) as u64);
                }
                Playout::Conceal => source.conceal(&mut self.scratch),
                Playout::Idle => break,
            }
            self.pcm.extend(self.scratch.iter());
        }
        if self.pcm.is_empty() {
            return None;
        }
        let mut frame = SILENCE;
        for (slot, s) in frame.iter_mut().zip(self.pcm.drain(..FRAME_SAMPLES.min(self.pcm.len()))) {
            *slot = s;
        }
        Some(frame)
    }

    fn detect(&mut self, config: &LegConfig, frame: Option<&Frame>) {
        let mut detections = std::mem::take(&mut self.detections);
        detections.clear();
        match frame {
            Some(f) => {
                self.quiet_ticks = 0;
                self.detector.push(f, &mut detections);
            }
            None => {
                self.quiet_ticks += 1;
                if self.quiet_ticks == AUDIO_GAP_TICKS {
                    detections.extend(self.detector.reset());
                }
            }
        }
        // A tone needs help only toward a peer that takes events; a peer that takes tones
        // already hears it in the audio.
        let takes_events = config.dtmf_peer.as_ref().is_some_and(|p| p.takes_events);
        for d in detections.drain(..) {
            match d {
                Detection::Start(event) => signal(config, PeerDtmf::Start(event), takes_events),
                Detection::End(event, ms) => {
                    self.report(&config.place, event, ms, dtmf::Source::Inband);
                    signal(config, PeerDtmf::End, takes_events);
                }
            }
        }
        self.detections = detections;
    }
}

impl Inbound {
    fn report(&mut self, place: &Place, event: u8, duration_ms: u32, source: dtmf::Source) {
        if let Some(digit) = event_to_char(event) {
            tracing::debug!(place = place.key(), ?source, duration_ms, "dtmf received");
            self.reported.push(Digit { digit, duration_ms, source });
        }
    }
}

/// Tells the peer about a digit. A muted leg's digits are reported, but not passed on.
fn signal(config: &LegConfig, dtmf: PeerDtmf, wanted: bool) {
    if let Some(peer) = &config.dtmf_peer
        && wanted
        && !config.muted
    {
        peer.leg.post(LegCmd::PeerDtmf(dtmf));
    }
}


