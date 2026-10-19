//! RFC 4733 telephone events. One key press is a run of packets sharing an RTP timestamp:
//! the first has the marker bit, each one carries the duration so far, and the last carries
//! the end bit (sent three times, for robustness).

use std::time::{Duration, Instant};

pub const PAYLOAD_LEN: usize = 4;
/// Volume field: 10 means −10 dBm0.
pub const VOLUME: u8 = 10;
/// An event with no packet for this long is over, even without its end packets.
pub const RECEIVE_TIMEOUT: Duration = Duration::from_millis(300);
const END_REPEATS: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    pub event: u8,
    pub end: bool,
    pub volume: u8,
    /// In RTP timestamp units since the event started.
    pub duration: u16,
}

impl Event {
    pub fn parse(payload: &[u8]) -> Option<Event> {
        let p = payload.get(..PAYLOAD_LEN)?;
        Some(Event {
            event: p[0],
            end: p[1] & 0x80 != 0,
            volume: p[1] & 0x3F,
            duration: u16::from_be_bytes([p[2], p[3]]),
        })
    }

    pub fn to_bytes(self) -> [u8; PAYLOAD_LEN] {
        let d = self.duration.to_be_bytes();
        [self.event, ((self.end as u8) << 7) | (self.volume & 0x3F), d[0], d[1]]
    }
}

/// What a received packet means for the key press it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Update {
    Start { event: u8 },
    Continue { event: u8, duration: u16 },
    End { event: u8, duration: u16 },
    /// A retransmitted end packet; the press was already reported.
    Repeat { event: u8, duration: u16 },
}

struct Current {
    timestamp: u32,
    event: u8,
    duration: u16,
    ended: bool,
    seen_at: Instant,
}

/// Turns received event packets into key presses.
#[derive(Default)]
pub struct Receiver {
    current: Option<Current>,
}

impl Receiver {
    pub fn on_packet(&mut self, timestamp: u32, ev: Event, now: Instant, updates: &mut Vec<Update>) {
        if let Some(c) = &mut self.current
            && c.timestamp == timestamp
        {
            c.seen_at = now;
            if c.ended {
                updates.push(Update::Repeat { event: c.event, duration: ev.duration });
                return;
            }
            c.duration = c.duration.max(ev.duration);
            if ev.end {
                c.ended = true;
                updates.push(Update::End { event: c.event, duration: c.duration });
            } else {
                updates.push(Update::Continue { event: c.event, duration: c.duration });
            }
            return;
        }
        // A new press. The previous one may have lost its end packets.
        if let Some(c) = &self.current
            && !c.ended
        {
            updates.push(Update::End { event: c.event, duration: c.duration });
        }
        self.current =
            Some(Current { timestamp, event: ev.event, duration: ev.duration, ended: ev.end, seen_at: now });
        updates.push(Update::Start { event: ev.event });
        if ev.end {
            updates.push(Update::End { event: ev.event, duration: ev.duration });
        }
    }

    /// Ends a press whose packets stopped coming.
    pub fn expire(&mut self, now: Instant) -> Option<Update> {
        let c = self.current.as_mut()?;
        if c.ended || now < c.seen_at + RECEIVE_TIMEOUT {
            return None;
        }
        c.ended = true;
        Some(Update::End { event: c.event, duration: c.duration })
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.current.as_ref().filter(|c| !c.ended).map(|c| c.seen_at + RECEIVE_TIMEOUT)
    }
}

/// Generates the packets of one key press, one update per tick.
pub struct Sender {
    event: u8,
    step: u32,
    limit: Option<u32>,
    duration: u32,
    started: bool,
    done: bool,
}

impl Sender {
    /// `step` is the duration of one tick in RTP units. With `total`, the press ends by
    /// itself; without, it lasts until `finish`.
    pub fn new(event: u8, step: u32, total: Option<u32>) -> Sender {
        Sender { event, step, limit: total.map(|t| t.max(step)), duration: 0, started: false, done: false }
    }

    pub fn done(&self) -> bool {
        self.done
    }

    pub fn duration(&self) -> u32 {
        self.duration
    }

    /// The packets for this tick, with the marker bit for each.
    pub fn tick(&mut self) -> Vec<(Event, bool)> {
        if self.done {
            return Vec::new();
        }
        let marker = !self.started;
        self.started = true;
        self.duration += self.step;
        if let Some(limit) = self.limit
            && self.duration >= limit
        {
            self.duration = limit;
            return self.end_packets();
        }
        vec![(self.packet(false), marker)]
    }

    /// Ends the press now: the three end packets.
    pub fn finish(&mut self) -> Vec<(Event, bool)> {
        if self.done {
            return Vec::new();
        }
        if !self.started {
            self.started = true;
            self.duration = self.step;
        }
        self.end_packets()
    }

    fn end_packets(&mut self) -> Vec<(Event, bool)> {
        self.done = true;
        vec![(self.packet(true), false); END_REPEATS]
    }

    fn packet(&self, end: bool) -> Event {
        Event { event: self.event, end, volume: VOLUME, duration: self.duration.min(u16::MAX as u32) as u16 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event: u8, end: bool, duration: u16) -> Event {
        Event { event, end, volume: 10, duration }
    }

    #[test]
    fn payload_round_trips() {
        let e = ev(11, true, 1280);
        assert_eq!(e.to_bytes(), [11, 0x8A, 0x05, 0x00]);
        assert_eq!(Event::parse(&e.to_bytes()), Some(e));
        assert_eq!(Event::parse(&[1, 2, 3]), None);
    }

    #[test]
    fn receiver_reports_each_press_once() {
        let (mut r, mut u, now) = (Receiver::default(), Vec::new(), Instant::now());
        r.on_packet(100, ev(5, false, 160), now, &mut u);
        r.on_packet(100, ev(5, false, 320), now, &mut u);
        for _ in 0..3 {
            r.on_packet(100, ev(5, true, 480), now, &mut u);
        }
        assert_eq!(
            u,
            [
                Update::Start { event: 5 },
                Update::Continue { event: 5, duration: 320 },
                Update::End { event: 5, duration: 480 },
                Update::Repeat { event: 5, duration: 480 },
                Update::Repeat { event: 5, duration: 480 },
            ]
        );
        assert_eq!(r.deadline(), None);
    }

    #[test]
    fn a_press_without_end_packets_still_ends() {
        let (mut r, mut u, now) = (Receiver::default(), Vec::new(), Instant::now());
        r.on_packet(100, ev(1, false, 160), now, &mut u);
        r.on_packet(900, ev(2, false, 160), now, &mut u);
        assert_eq!(u[1..], [Update::End { event: 1, duration: 160 }, Update::Start { event: 2 }]);

        assert_eq!(r.expire(now), None);
        let later = now + RECEIVE_TIMEOUT;
        assert_eq!(r.deadline(), Some(later));
        assert_eq!(r.expire(later), Some(Update::End { event: 2, duration: 160 }));
        assert_eq!(r.expire(later), None);
    }

    #[test]
    fn sender_paces_a_fixed_press_and_ends_three_times() {
        let mut s = Sender::new(9, 160, Some(800));
        let packets: Vec<Vec<(Event, bool)>> = (0..6).map(|_| s.tick()).collect();
        assert_eq!(packets[0], [(ev(9, false, 160), true)]);
        assert_eq!(packets[3], [(ev(9, false, 640), false)]);
        assert_eq!(packets[4], vec![(ev(9, true, 800), false); 3]);
        assert!(packets[5].is_empty() && s.done());
    }

    #[test]
    fn open_press_ends_on_finish() {
        let mut s = Sender::new(0, 960, None);
        s.tick();
        s.tick();
        assert_eq!(s.finish(), vec![(ev(0, true, 1920), false); 3]);
        assert!(s.done() && s.tick().is_empty() && s.finish().is_empty());
    }
}
