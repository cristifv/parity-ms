//! Each leg publishes the audio it receives as 20 ms frames in a small ring. The other legs
//! of its call or conference read it with their own cursor and mix it into what they send.
//! A leg never hears itself: everyone gets the mix of the others (N−1).

use std::sync::Mutex;

use crate::codec::FRAME_SAMPLES;

pub type Frame = [i16; FRAME_SAMPLES];

pub const SILENCE: Frame = [0; FRAME_SAMPLES];
/// Frames kept: 160 ms.
const RING: u64 = 8;
/// A reader further behind than this skips ahead, which bounds the added latency.
const MAX_BACKLOG: u64 = 3;

struct Ring {
    frames: [Frame; RING as usize],
    written: u64,
}

pub struct FrameRing {
    inner: Mutex<Ring>,
}

impl Default for FrameRing {
    fn default() -> Self {
        FrameRing { inner: Mutex::new(Ring { frames: [SILENCE; RING as usize], written: 0 }) }
    }
}

impl FrameRing {
    pub fn write(&self, frame: &Frame) {
        let mut r = self.inner.lock().unwrap();
        let slot = (r.written % RING) as usize;
        r.frames[slot] = *frame;
        r.written += 1;
    }

    /// Where a new reader starts: at the newest frame, with no backlog.
    pub fn cursor(&self) -> u64 {
        self.inner.lock().unwrap().written
    }

    /// The next frame for the reader at `cursor`, if one has been written since.
    pub fn read(&self, cursor: &mut u64) -> Option<Frame> {
        let r = self.inner.lock().unwrap();
        if *cursor >= r.written {
            return None;
        }
        if r.written - *cursor > MAX_BACKLOG {
            *cursor = r.written - 1;
        }
        let frame = r.frames[(*cursor % RING) as usize];
        *cursor += 1;
        Some(frame)
    }
}

/// Sums frames, clipping at the 16-bit limits.
#[derive(Clone)]
pub struct Mix {
    acc: [i32; FRAME_SAMPLES],
    sources: usize,
}

impl Default for Mix {
    fn default() -> Self {
        Mix { acc: [0; FRAME_SAMPLES], sources: 0 }
    }
}

impl Mix {
    pub fn add(&mut self, frame: &Frame) {
        for (a, &s) in self.acc.iter_mut().zip(frame) {
            *a += s as i32;
        }
        self.sources += 1;
    }

    pub fn sources(&self) -> usize {
        self.sources
    }

    pub fn frame(&self) -> Frame {
        let mut out = SILENCE;
        for (o, &a) in out.iter_mut().zip(&self.acc) {
            *o = a.clamp(i16::MIN as i32, i16::MAX as i32) as i16;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_follow_the_writer_with_bounded_backlog() {
        let ring = FrameRing::default();
        let mut late = ring.cursor();
        let mut early = ring.cursor();
        assert_eq!(ring.read(&mut early), None);
        for i in 0..6 {
            ring.write(&[i; FRAME_SAMPLES]);
            if i == 0 {
                assert_eq!(ring.read(&mut early).unwrap()[0], 0);
            }
        }
        // `late` is 6 frames behind: it skips to the newest.
        assert_eq!(ring.read(&mut late).unwrap()[0], 5);
        assert_eq!(ring.read(&mut late), None);
        // `early` is 5 behind: also skips.
        assert_eq!(ring.read(&mut early).unwrap()[0], 5);
    }

    #[test]
    fn mixing_adds_and_clips() {
        let mut m = Mix::default();
        m.add(&[20000; FRAME_SAMPLES]);
        m.add(&[20000; FRAME_SAMPLES]);
        m.add(&[-100; FRAME_SAMPLES]);
        assert_eq!(m.sources(), 3);
        assert_eq!(m.frame()[0], i16::MAX);
        let mut quiet = Mix::default();
        quiet.add(&[100; FRAME_SAMPLES]);
        quiet.add(&[-30; FRAME_SAMPLES]);
        assert_eq!(quiet.frame()[7], 70);
    }
}
