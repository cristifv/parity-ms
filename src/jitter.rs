//! Adaptive jitter buffer. It holds a leg's incoming RTP packets long enough to put them
//! back in order and absorb network jitter, then hands them out in timestamp order for
//! decoding, one play position at a time.
//!
//! The delay follows the measured interarrival jitter (RFC 3550 §6.4.1):
//! - **Target:** about one frame plus four times the jitter, between `min_delay` (20 ms by
//!   default) and `max_delay`.
//! - **Grows:** when the buffer runs dry, the play position waits (the gap is concealed),
//!   which adds a frame of delay.
//! - **Shrinks:** when the buffer holds well over its target, a frame is skipped now and
//!   then.
//!
//! The play-out delay stays short, but the buffer can hold much more, `capacity_bytes`
//! (192,000 by default): late bursts and reordered packets are kept rather than thrown away.
//! The oldest packets go first when it's full.
//!
//! Only the decoding paths (mixing, transcoding, recording, tone detection) use it.
//! Passthrough forwards packets as they arrive.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::Serialize;

#[derive(Debug, Clone, Copy)]
pub struct JitterConfig {
    /// Starting (and smallest) play-out delay.
    pub min_delay: Duration,
    pub max_delay: Duration,
    /// Payload bytes the buffer may hold.
    pub capacity_bytes: usize,
}

pub const DEFAULT_CAPACITY_BYTES: usize = 192_000;

impl Default for JitterConfig {
    fn default() -> Self {
        JitterConfig {
            min_delay: Duration::from_millis(20),
            max_delay: Duration::from_millis(200),
            capacity_bytes: DEFAULT_CAPACITY_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    /// Extended (wrap-free) RTP timestamp.
    pub timestamp: u64,
    pub sequence: u16,
    pub payload: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Playout {
    /// The packet at the play position: decode it, then call `played`.
    Packet(Packet),
    /// Audio at the play position is missing (lost, or not here yet): conceal one frame.
    Conceal,
    /// Nothing to play: still buffering, or the stream has paused.
    Idle,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct JitterStats {
    pub received: u64,
    /// Arrived after their play position had passed.
    pub late: u64,
    pub duplicate: u64,
    /// Frames concealed because their packet never came.
    pub lost: u64,
    /// Frames concealed while waiting for a packet that was late, which adds delay.
    pub stretched: u64,
    /// Frames skipped to bring the delay back down.
    pub skipped: u64,
    /// Oldest packets dropped because the buffer was full.
    pub overflow: u64,
    /// Payload bytes held now.
    pub buffered_bytes: usize,
    pub jitter_ms: f64,
    pub target_delay_ms: f64,
}

pub struct JitterBuffer {
    clock: u32,
    frame: u64,
    min_delay: u64,
    max_delay: u64,
    capacity: usize,
    bytes: usize,
    packets: BTreeMap<u64, Packet>,
    /// Newest timestamp seen (raw and extended), for unwrapping.
    newest: Option<(u32, u64)>,
    /// Extended timestamp of the next audio to play.
    playout: Option<u64>,
    /// When buffering started.
    buffering_since: Option<Instant>,
    epoch: Instant,
    /// Interarrival jitter, in RTP units.
    jitter: f64,
    last_transit: Option<f64>,
    dry_frames: u64,
    pops_since_skip: u32,
    stats: JitterStats,
}

impl JitterBuffer {
    /// `clock` is the RTP clock rate; one frame is 20 ms.
    pub fn new(clock: u32, config: JitterConfig) -> JitterBuffer {
        let ticks = |d: Duration| (d.as_secs_f64() * clock as f64) as u64;
        JitterBuffer {
            clock,
            frame: clock as u64 / 50,
            min_delay: ticks(config.min_delay),
            max_delay: ticks(config.max_delay).max(ticks(config.min_delay)),
            capacity: config.capacity_bytes.max(1),
            bytes: 0,
            packets: BTreeMap::new(),
            newest: None,
            playout: None,
            buffering_since: None,
            epoch: Instant::now(),
            jitter: 0.0,
            last_transit: None,
            dry_frames: 0,
            pops_since_skip: 0,
            stats: JitterStats::default(),
        }
    }

    pub fn clock(&self) -> u32 {
        self.clock
    }

    pub fn stats(&self) -> JitterStats {
        let ms = |ticks: f64| ticks * 1000.0 / self.clock as f64;
        JitterStats {
            jitter_ms: ms(self.jitter),
            target_delay_ms: ms(self.target() as f64),
            buffered_bytes: self.bytes,
            ..self.stats
        }
    }

    /// Current target delay, in RTP units.
    fn target(&self) -> u64 {
        (self.frame + (4.0 * self.jitter) as u64).clamp(self.min_delay, self.max_delay)
    }

    /// Forgets everything, e.g. when the sender's SSRC changes.
    pub fn reset(&mut self) {
        self.packets.clear();
        self.bytes = 0;
        self.newest = None;
        self.playout = None;
        self.buffering_since = None;
        self.last_transit = None;
        self.dry_frames = 0;
    }

    fn extend(&mut self, ts: u32) -> u64 {
        let ext = match self.newest {
            // Half the 32-bit range either way decides between wrap-around and reordering.
            Some((raw, ext)) => (ext as i64 + ts.wrapping_sub(raw) as i32 as i64).max(0) as u64,
            None => ts as u64 + (1 << 32), // headroom for packets older than the first
        };
        if self.newest.is_none_or(|(_, newest)| ext > newest) {
            self.newest = Some((ts, ext));
        }
        ext
    }

    pub fn push(&mut self, timestamp: u32, sequence: u16, payload: &[u8], arrival: Instant) {
        let ts = self.extend(timestamp);
        let transit = arrival.duration_since(self.epoch).as_secs_f64() * self.clock as f64 - ts as f64;
        if let Some(last) = self.last_transit {
            self.jitter += ((transit - last).abs() - self.jitter) / 16.0;
        }
        self.last_transit = Some(transit);
        self.stats.received += 1;

        if let Some(pos) = self.playout {
            if ts < pos {
                self.stats.late += 1;
                return;
            }
            if ts > pos + self.clock as u64 {
                // More than a second ahead: the stream jumped. Start over from here.
                self.packets.clear();
                self.bytes = 0;
                self.playout = None;
            }
        }
        if self.packets.contains_key(&ts) {
            self.stats.duplicate += 1;
            return;
        }
        self.bytes += payload.len();
        self.packets.insert(ts, Packet { timestamp: ts, sequence, payload: payload.to_vec() });
        if self.playout.is_none() && self.buffering_since.is_none() {
            self.buffering_since = Some(arrival);
        }
        while self.bytes > self.capacity {
            let Some((_, oldest)) = self.packets.pop_first() else { break };
            self.bytes -= oldest.payload.len();
            self.stats.overflow += 1;
        }
    }

    fn take(&mut self, ts: u64) -> Option<Packet> {
        let p = self.packets.remove(&ts)?;
        self.bytes -= p.payload.len();
        Some(p)
    }

    /// What plays next. Called whenever the decoder needs another frame of audio.
    pub fn pop(&mut self, now: Instant) -> Playout {
        let pos = match self.playout {
            Some(pos) => pos,
            None => {
                let (Some(since), Some((&first, _))) = (self.buffering_since, self.packets.first_key_value()) else {
                    return Playout::Idle;
                };
                let waited = (now.saturating_duration_since(since).as_secs_f64() * self.clock as f64) as u64;
                if waited < self.target() {
                    return Playout::Idle;
                }
                self.buffering_since = None;
                self.playout = Some(first);
                first
            }
        };
        while let Some((&ts, _)) = self.packets.first_key_value() {
            if ts >= pos {
                break;
            }
            self.take(ts);
            self.stats.late += 1;
        }

        // Too much queued: skip a frame now and then to bring the delay down.
        self.pops_since_skip += 1;
        if let Some((&newest, _)) = self.packets.last_key_value()
            && newest + self.frame - pos > self.target() + 2 * self.frame
            && self.pops_since_skip >= 5
            && self.packets.len() > 1
            && let Some((&ts, _)) = self.packets.first_key_value()
            && ts == pos
        {
            self.take(ts);
            self.stats.skipped += 1;
            self.pops_since_skip = 0;
            let next = self.packets.first_key_value().map(|(&t, _)| t).unwrap_or(pos + self.frame);
            self.playout = Some(next);
            return self.pop(now);
        }

        match self.packets.first_key_value().map(|(&ts, _)| ts) {
            Some(ts) if ts == pos => {
                self.dry_frames = 0;
                Playout::Packet(self.take(ts).expect("present"))
            }
            Some(ts) => {
                // A gap before the next packet: that audio was lost.
                self.dry_frames = 0;
                self.stats.lost += 1;
                self.playout = Some(pos + self.frame.min(ts - pos));
                Playout::Conceal
            }
            None => {
                // Nothing yet: wait in place, which adds a frame of delay. After a long
                // dry spell the stream has paused (silence suppression, hold); start over.
                self.dry_frames += 1;
                if self.dry_frames * self.frame > self.max_delay {
                    self.playout = None;
                    self.dry_frames = 0;
                    return Playout::Idle;
                }
                self.stats.stretched += 1;
                Playout::Conceal
            }
        }
    }

    /// After decoding a packet from `pop`: it covered `ticks` RTP units.
    pub fn played(&mut self, packet_timestamp: u64, ticks: u64) {
        self.playout = Some(packet_timestamp + ticks.max(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn jb() -> JitterBuffer {
        JitterBuffer::new(8000, JitterConfig::default())
    }

    /// Pops until Idle (at most `n` times), decoding 20 ms per packet; returns what played.
    fn drain(b: &mut JitterBuffer, now: Instant, n: usize) -> Vec<Option<u64>> {
        let mut out = Vec::new();
        for _ in 0..n {
            match b.pop(now) {
                Playout::Packet(p) => {
                    b.played(p.timestamp, 160);
                    out.push(Some(p.timestamp - (1 << 32)));
                }
                Playout::Conceal => out.push(None),
                Playout::Idle => break,
            }
        }
        out
    }

    #[test]
    fn waits_for_the_target_delay_then_plays_in_order() {
        let (mut b, t0) = (jb(), Instant::now());
        b.push(160, 2, b"b", t0);
        b.push(0, 1, b"a", t0);
        assert_eq!(b.pop(t0), Playout::Idle, "still buffering");
        // Two packets at once is jitter, so the target is a little over 20 ms.
        assert_eq!(drain(&mut b, t0 + 40 * MS, 2), [Some(0), Some(160)], "reordered");
    }

    #[test]
    fn conceals_losses_and_counts_late_and_duplicate_packets() {
        let (mut b, t0) = (jb(), Instant::now());
        for (ts, seq) in [(0, 1), (320, 3), (320, 3)] {
            b.push(ts, seq, b"x", t0);
        }
        assert_eq!(drain(&mut b, t0 + 50 * MS, 3), [Some(0), None, Some(320)]);
        b.push(160, 2, b"x", t0 + 60 * MS);
        let s = b.stats();
        assert_eq!((s.received, s.duplicate, s.lost, s.late), (4, 1, 1, 1));
    }

    #[test]
    fn a_dry_buffer_stretches_then_pauses() {
        let (mut b, t0) = (jb(), Instant::now());
        b.push(0, 1, b"x", t0);
        assert_eq!(drain(&mut b, t0 + 20 * MS, 1), [Some(0)]);
        // Nothing arrives: conceal in place (adds delay) ...
        assert_eq!(b.pop(t0 + 40 * MS), Playout::Conceal);
        // ... and the late packet still plays where it belongs.
        b.push(160, 2, b"x", t0 + 45 * MS);
        assert_eq!(drain(&mut b, t0 + 60 * MS, 1), [Some(160)]);
        assert_eq!(b.stats().stretched, 1);

        // A long silence: back to buffering, and a jump ahead starts a new run.
        let mut pops = 0;
        while b.pop(t0 + 80 * MS) == Playout::Conceal {
            pops += 1;
        }
        assert!(pops > 5, "about max_delay of concealment first: {pops}");
        b.push(160 + 8000 * 5, 3, b"x", t0 + 5000 * MS);
        assert_eq!(drain(&mut b, t0 + 5040 * MS, 1), [Some(160 + 8000 * 5)]);
    }

    #[test]
    fn handles_timestamp_wrap() {
        let (mut b, t0) = (jb(), Instant::now());
        b.push(u32::MAX - 159, 1, b"x", t0);
        b.push(0, 2, b"x", t0);
        let played = drain(&mut b, t0 + 50 * MS, 2);
        assert_eq!(played.len(), 2);
        assert_eq!(played[1].unwrap() - played[0].unwrap(), 160);
    }

    #[test]
    fn delay_adapts_to_jitter() {
        let (mut b, t0) = (jb(), Instant::now());
        // Steady arrivals: the target stays at the minimum.
        for i in 0..50u32 {
            b.push(i * 160, i as u16, b"x", t0 + i * 20 * MS);
        }
        assert_eq!(b.stats().target_delay_ms, 20.0);
        // Arrivals swinging by ±15 ms: the jitter estimate and the target grow.
        for i in 50..150u32 {
            let swing = if i % 2 == 0 { 15 } else { 0 };
            b.push(i * 160, i as u16, b"x", t0 + (i * 20 + swing) * MS);
        }
        let s = b.stats();
        assert!(s.jitter_ms > 5.0, "{s:?}");
        assert!(s.target_delay_ms > 40.0 && s.target_delay_ms <= 200.0, "{s:?}");
    }

    #[test]
    fn skips_frames_when_far_behind() {
        let (mut b, t0) = (jb(), Instant::now());
        for i in 0..25u32 {
            b.push(i * 160, i as u16, b"x", t0);
        }
        let played: Vec<u64> = drain(&mut b, t0 + 200 * MS, 25).into_iter().flatten().collect();
        let skipped = b.stats().skipped;
        assert!(skipped >= 2, "a 500 ms backlog is trimmed: {skipped}");
        assert_eq!(b.stats().overflow, 0, "plenty of room");
        assert_eq!(played.len() as u64 + skipped, 25, "every packet either played or skipped");
        assert!(played.windows(2).all(|w| w[0] < w[1]), "still in order");
    }

    #[test]
    fn holds_up_to_its_byte_capacity() {
        let config = JitterConfig { capacity_bytes: 1000, ..JitterConfig::default() };
        let (mut b, t0) = (JitterBuffer::new(8000, config), Instant::now());
        for i in 0..10u32 {
            b.push(i * 160, i as u16, &[0; 160], t0);
        }
        let s = b.stats();
        assert_eq!((s.overflow, s.buffered_bytes), (4, 960), "six 160-byte packets fit in 1000 bytes");
        assert_eq!(drain(&mut b, t0 + 200 * MS, 1), [Some(4 * 160)], "the oldest went first");

        let default = JitterBuffer::new(8000, JitterConfig::default());
        assert_eq!(default.capacity, 192_000);
        assert_eq!(default.stats().target_delay_ms, 20.0);
    }
}
