//! The sending side of a leg, when its stream is made here.
//! - **Every 20 ms:** mix the peers' latest frames (everyone hears the others, not
//!   themselves), apply any DTMF being generated, encode with the leg's transcoding
//!   [`Destination`] (its codec), and send.
//! - **Lonely participant:** with nobody else to hear, the leg gets silence, which keeps its
//!   stream, NAT bindings and RTP timeouts alive.
//! - **Playing a file:** its next 20 ms are mixed into what the leg hears, so they reach
//!   the leg's recording frame by frame too.
//! - **Every 5 s:** an RTCP Sender Report, so the leg can map the stream's RTP timestamps
//!   to wall-clock time.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{Injected, LegConfig, LegShared, PeerDtmf, PlayEnd, PlayFinished, PlayStatus};
use crate::codec::{FRAME_SAMPLES, PCM_RATE};
use crate::dtmf::inband::ToneGen;
use crate::dtmf::rfc4733::Sender;
use crate::media::mixer::{Frame, Mix, SILENCE};
use crate::media::prompt::Prompt;
use crate::media::transcode::Destination;

/// A tone made from the peer's events stops after this long, even if the peer never ends it.
const MAX_CONVERTED: Duration = Duration::from_secs(5);
/// Ticks between RTCP Sender Reports (5 s). The first goes out after 1 s.
const REPORT_TICKS: u32 = 250;
const FIRST_REPORT_TICKS: u32 = 50;

pub struct Outbound {
    destination: Option<Destination>,
    ticks_since_report: u32,
    /// Read position in each peer's frames, keyed by the peer.
    cursors: HashMap<usize, u64>,
    dtmf: DtmfOut,
    playing: Option<Playing>,
    /// Files that ended, until the leg reports them.
    finished: Vec<PlayFinished>,
}

/// A file being played, and how far it has got.
struct Playing {
    prompt: Arc<Prompt>,
    /// Samples played.
    position: usize,
}

impl Playing {
    fn next_frame(&mut self) -> Frame {
        let mut frame = SILENCE;
        let pcm = &self.prompt.pcm;
        let end = pcm.len().min(self.position + FRAME_SAMPLES);
        if self.position < end {
            frame[..end - self.position].copy_from_slice(&pcm[self.position..end]);
        }
        self.position += FRAME_SAMPLES;
        frame
    }

    fn done(&self) -> bool {
        self.position >= self.prompt.pcm.len()
    }

    fn status(&self) -> PlayStatus {
        PlayStatus {
            file: self.prompt.name.clone(),
            position_ms: (self.position.min(self.prompt.pcm.len()) as u64) * 1000 / PCM_RATE as u64,
            duration_ms: self.prompt.duration_ms(),
        }
    }

    fn end(self, reason: PlayEnd) -> PlayFinished {
        let status = self.status();
        PlayFinished { file: status.file, reason, played_ms: status.position_ms, duration_ms: status.duration_ms }
    }
}

impl Outbound {
    pub fn new() -> Outbound {
        Outbound {
            destination: None,
            ticks_since_report: REPORT_TICKS - FIRST_REPORT_TICKS,
            cursors: HashMap::new(),
            dtmf: DtmfOut::default(),
            playing: None,
            finished: Vec::new(),
        }
    }

    /// Starts playing a file into the leg, in place of one still playing.
    pub fn play(&mut self, prompt: Arc<Prompt>) {
        if let Some(old) = self.playing.take() {
            self.finished.push(old.end(PlayEnd::Replaced));
        }
        self.playing = Some(Playing { prompt, position: 0 });
    }

    pub fn stop_play(&mut self) {
        if let Some(p) = self.playing.take() {
            self.finished.push(p.end(PlayEnd::Stopped));
        }
    }

    pub fn playing(&self) -> Option<PlayStatus> {
        self.playing.as_ref().map(Playing::status)
    }

    /// Files that ended since the last call.
    pub fn take_finished(&mut self) -> Vec<PlayFinished> {
        std::mem::take(&mut self.finished)
    }

    pub fn configure(&mut self, config: &LegConfig) {
        let want = config
            .generate
            .then(|| config.codec.as_ref().and_then(|c| c.supported.map(|codec| (codec, c.pt))))
            .flatten();
        match (want, &self.destination) {
            (Some((codec, pt)), Some(s)) if s.codec() == codec && s.pt() == pt => {}
            (Some((codec, pt)), _) => {
                self.ticks_since_report = REPORT_TICKS - FIRST_REPORT_TICKS;
                self.destination = Destination::new(codec, pt)
                    .inspect_err(|e| tracing::warn!(error = %e, "can't generate this leg's stream"))
                    .ok();
            }
            (None, _) => {
                self.destination = None;
                self.dtmf = DtmfOut::default();
            }
        }
        let peers: Vec<usize> = config.peers.iter().map(key).collect();
        self.cursors.retain(|k, _| peers.contains(k));
    }

    /// Private API: `frames` frames were missed; the leg's stream skips them, and so does
    /// a file being played, which keeps to wall-clock time.
    pub(crate) fn fill_blanks(&mut self, frames: u32) {
        if let Some(destination) = &mut self.destination {
            destination.skip_frames(frames);
        }
        if let Some(p) = &mut self.playing {
            p.position += frames as usize * FRAME_SAMPLES;
            if p.done() {
                let p = self.playing.take().expect("playing");
                self.finished.push(p.end(PlayEnd::Completed));
            }
        }
    }

    pub fn enqueue_dtmf(&mut self, digits: Vec<Injected>) {
        self.dtmf.pending.extend(digits);
    }

    /// DTMF from the call's other leg, to reproduce in this leg's stream.
    pub fn peer_dtmf(&mut self, dtmf: PeerDtmf, config: &LegConfig, shared: &LegShared, now: Instant) {
        let Some(destination) = &mut self.destination else { return };
        let te = config.codec.as_ref().and_then(|c| c.dtmf);
        for packet in self.dtmf.peer(dtmf, now, destination, te) {
            shared.send_rtp(&packet);
        }
    }

    /// One 20 ms tick. Returns what the leg hears this tick, for its recording.
    pub fn tick(&mut self, config: &LegConfig, shared: &LegShared, now: Instant) -> Option<Frame> {
        let mut mix = Mix::default();
        for peer in &config.peers {
            let cursor = self.cursors.entry(key(peer)).or_insert_with(|| peer.frames.cursor());
            if let Some(frame) = peer.frames.read(cursor) {
                mix.add(&frame);
            }
        }
        let Some(destination) = &mut self.destination else {
            // Not made here: the peer's packets are forwarded to this leg as they are.
            return (mix.sources() > 0).then(|| mix.frame());
        };
        if let Some(p) = &mut self.playing {
            mix.add(&p.next_frame());
            if p.done() {
                let p = self.playing.take().expect("playing");
                self.finished.push(p.end(PlayEnd::Completed));
            }
        }
        let mut frame = mix.frame();
        let te = config.codec.as_ref().and_then(|c| c.dtmf);
        match self.dtmf.tick(now, destination, te, &mut frame) {
            Some(events) => {
                for packet in events {
                    shared.send_rtp(&packet);
                }
                destination.skip_frame();
            }
            None => {
                if shared.rtp.remote.addr().is_some() {
                    if let Some(packet) = destination.frame(&frame) {
                        shared.send_rtp(&packet);
                    }
                } else {
                    destination.skip_frame();
                }
            }
        }
        self.ticks_since_report += 1;
        if self.ticks_since_report >= REPORT_TICKS && destination.packets_sent() > 0 {
            self.ticks_since_report = 0;
            shared.send_rtcp(&destination.sender_report(&format!("parity-ms@{}", shared.port)));
        }
        Some(frame)
    }
}

fn key(peer: &Arc<LegShared>) -> usize {
    Arc::as_ptr(peer) as usize
}

#[derive(Clone, Copy)]
enum Origin {
    /// Reproduces a digit the other leg is sending; ends when that digit ends.
    Converted,
    /// From `send_dtmf`; followed by a pause.
    Injected { pause: Duration },
}

#[derive(Default)]
enum Outgoing {
    #[default]
    Idle,
    Event { sender: Sender, pt: u8, timestamp: u32, origin: Origin },
    Tone { tone: ToneGen, samples_left: Option<usize>, stop_at: Instant, origin: Origin },
    Pause { until: Instant },
}

/// DTMF generated into a leg's stream: RFC 4733 events if the leg negotiated them, tones in
/// the audio otherwise. The mix is muted while a digit plays.
#[derive(Default)]
struct DtmfOut {
    pending: VecDeque<Injected>,
    outgoing: Outgoing,
}

impl DtmfOut {
    fn peer(&mut self, dtmf: PeerDtmf, now: Instant, destination: &mut Destination, te: Option<u8>) -> Vec<Vec<u8>> {
        match dtmf {
            PeerDtmf::Start(event) => {
                if matches!(self.outgoing, Outgoing::Idle | Outgoing::Pause { .. }) {
                    self.start(event, None, Origin::Converted, now, destination, te);
                }
                Vec::new()
            }
            PeerDtmf::End => match self.outgoing {
                Outgoing::Event { origin: Origin::Converted, .. } | Outgoing::Tone { origin: Origin::Converted, .. } => {
                    self.finish(now, destination)
                }
                _ => Vec::new(),
            },
        }
    }

    fn start(&mut self, event: u8, duration: Option<Duration>, origin: Origin, now: Instant, destination: &mut Destination, te: Option<u8>) {
        self.outgoing = match te {
            Some(pt) => {
                let total = duration.map(|d| (d.as_millis() as u64 * destination.codec().clock_rate() as u64 / 1000) as u32);
                let sender = Sender::new(event, destination.frame_ticks(), total);
                Outgoing::Event { sender, pt, timestamp: destination.position(), origin }
            }
            None => match ToneGen::new(event) {
                Some(tone) => {
                    let samples = duration.map(|d| (d.as_millis() as usize * PCM_RATE as usize / 1000).max(FRAME_SAMPLES));
                    let stop_at = now + duration.unwrap_or(MAX_CONVERTED);
                    Outgoing::Tone { tone, samples_left: samples, stop_at, origin }
                }
                None => Outgoing::Idle,
            },
        };
    }

    /// One tick. `Some(packets)`: an event is playing, send these instead of audio. `None`:
    /// send `frame`, which a tone may have replaced.
    fn tick(&mut self, now: Instant, destination: &mut Destination, te: Option<u8>, frame: &mut Frame) -> Option<Vec<Vec<u8>>> {
        if let Outgoing::Pause { until } = self.outgoing
            && now >= until
        {
            self.outgoing = Outgoing::Idle;
        }
        if matches!(self.outgoing, Outgoing::Idle)
            && let Some(next) = self.pending.pop_front()
        {
            self.start(next.event, Some(next.duration), Origin::Injected { pause: next.pause }, now, destination, te);
        }
        match &mut self.outgoing {
            Outgoing::Event { sender, pt, timestamp, .. } => {
                let (pt, ts) = (*pt, *timestamp);
                let mut packets: Vec<Vec<u8>> =
                    sender.tick().into_iter().map(|(ev, marker)| destination.event_packet(pt, ev, marker, ts)).collect();
                if sender.done() {
                    packets.extend(self.finish(now, destination));
                }
                Some(packets)
            }
            Outgoing::Tone { tone, samples_left, stop_at, .. } => {
                tone.fill(frame);
                if let Some(left) = samples_left {
                    *left = left.saturating_sub(FRAME_SAMPLES);
                }
                if samples_left.is_some_and(|l| l == 0) || now >= *stop_at {
                    self.finish(now, destination);
                }
                None
            }
            Outgoing::Idle | Outgoing::Pause { .. } => None,
        }
    }

    /// Ends the digit being played; returns any end packets still to send.
    fn finish(&mut self, now: Instant, destination: &mut Destination) -> Vec<Vec<u8>> {
        let (packets, origin) = match std::mem::take(&mut self.outgoing) {
            Outgoing::Event { mut sender, pt, timestamp, origin } => {
                let packets = sender.finish().into_iter().map(|(ev, m)| destination.event_packet(pt, ev, m, timestamp)).collect();
                (packets, origin)
            }
            Outgoing::Tone { origin, .. } => (Vec::new(), origin),
            other => {
                self.outgoing = other;
                return Vec::new();
            }
        };
        destination.mark();
        if let Origin::Injected { pause } = origin {
            self.outgoing = Outgoing::Pause { until: now + pause };
        }
        packets
    }
}
