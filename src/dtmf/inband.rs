//! DTMF tones inside the audio (8 kHz PCM): detection with the Goertzel algorithm, and
//! generation.

use std::f64::consts::TAU;

use super::{COL_HZ, ROW_HZ, from_keypad, keypad};
use crate::codec::PCM_RATE;

/// Samples per analysis block: 25.6 ms, the classic size for DTMF at 8 kHz.
const BLOCK: usize = 205;
const BLOCK_MS: u32 = (BLOCK as u32 * 1000) / PCM_RATE;
/// Blocks in a row needed to start, and to end, a digit (about 50 ms each).
const DEBOUNCE: u32 = 2;
/// Quietest tone that counts (about −40 dBFS).
const MIN_AMPLITUDE: f64 = 300.0;
/// The column tone may be up to 8 dB louder than the row tone, or 4 dB quieter.
const MAX_TWIST_DB: f64 = 8.0;
const MAX_REVERSE_TWIST_DB: f64 = 4.0;
/// The two tones must stand 6 dB above the other frequencies of their group…
const PEAK_RATIO: f64 = 4.0;
/// …and carry most of the block's energy, which tells them apart from speech and music.
const MIN_PURITY: f64 = 0.7;
/// Amplitude of each generated tone (about −13 dBFS).
const TONE_AMPLITUDE: f64 = 7000.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detection {
    Start(u8),
    End(u8, u32),
}

#[derive(Default)]
pub struct Detector {
    block: Vec<i16>,
    /// Digit being heard, with its block count.
    active: Option<(u8, u32)>,
    misses: u32,
    candidate: Option<(u8, u32)>,
}

impl Detector {
    pub fn push(&mut self, pcm: &[i16], out: &mut Vec<Detection>) {
        for &s in pcm {
            self.block.push(s);
            if self.block.len() == BLOCK {
                let hit = classify(&self.block);
                self.block.clear();
                self.step(hit, out);
            }
        }
    }

    pub fn active(&self) -> Option<u8> {
        self.active.map(|(d, _)| d)
    }

    /// Ends the digit being heard, e.g. because the audio stopped.
    pub fn reset(&mut self) -> Option<Detection> {
        self.block.clear();
        self.candidate = None;
        self.misses = 0;
        self.active.take().map(|(d, n)| Detection::End(d, n * BLOCK_MS))
    }

    fn step(&mut self, hit: Option<u8>, out: &mut Vec<Detection>) {
        if let Some((digit, blocks)) = &mut self.active {
            if hit == Some(*digit) {
                *blocks += 1;
                self.misses = 0;
                return;
            }
            self.misses += 1;
            if self.misses < DEBOUNCE {
                return;
            }
            out.push(Detection::End(*digit, *blocks * BLOCK_MS));
            self.active = None;
            self.misses = 0;
            self.candidate = hit.map(|h| (h, 1));
            return;
        }
        self.candidate = match (hit, self.candidate) {
            (Some(h), Some((c, n))) if h == c => Some((h, n + 1)),
            (Some(h), _) => Some((h, 1)),
            (None, _) => None,
        };
        if let Some((digit, n)) = self.candidate
            && n >= DEBOUNCE
        {
            self.active = Some((digit, n));
            self.candidate = None;
            out.push(Detection::Start(digit));
        }
    }
}

fn goertzel(block: &[i16], hz: f64) -> f64 {
    let coeff = 2.0 * (TAU * hz / PCM_RATE as f64).cos();
    let (mut s1, mut s2) = (0.0f64, 0.0f64);
    for &x in block {
        let s0 = x as f64 + coeff * s1 - s2;
        s2 = s1;
        s1 = s0;
    }
    s1 * s1 + s2 * s2 - coeff * s1 * s2
}

fn strongest(powers: &[f64; 4]) -> Option<(usize, f64)> {
    let (i, &p) = powers.iter().enumerate().max_by(|a, b| a.1.total_cmp(b.1))?;
    let clear = powers.iter().enumerate().all(|(j, &q)| j == i || q * PEAK_RATIO <= p);
    clear.then_some((i, p))
}

/// The digit in one block, if the block is a clean DTMF tone pair.
fn classify(block: &[i16]) -> Option<u8> {
    let n = block.len() as f64;
    let energy: f64 = block.iter().map(|&x| (x as f64).powi(2)).sum();
    if energy == 0.0 {
        return None;
    }
    let rows = ROW_HZ.map(|f| goertzel(block, f));
    let cols = COL_HZ.map(|f| goertzel(block, f));
    let (row, row_power) = strongest(&rows)?;
    let (col, col_power) = strongest(&cols)?;
    // A sine of amplitude A shows up with power (A·N/2)².
    let amplitude = |p: f64| 2.0 * p.sqrt() / n;
    let (row_amp, col_amp) = (amplitude(row_power), amplitude(col_power));
    if row_amp < MIN_AMPLITUDE || col_amp < MIN_AMPLITUDE {
        return None;
    }
    let twist_db = 20.0 * (col_amp / row_amp).log10();
    if !(-MAX_REVERSE_TWIST_DB..=MAX_TWIST_DB).contains(&twist_db) {
        return None;
    }
    // For a pure tone pair, (row + col power) equals energy·N/2.
    if (row_power + col_power) / (energy * n / 2.0) < MIN_PURITY {
        return None;
    }
    Some(from_keypad(row, col))
}

/// A continuous DTMF tone pair.
pub struct ToneGen {
    row: f64,
    col: f64,
    sample: u64,
}

impl ToneGen {
    pub fn new(event: u8) -> Option<ToneGen> {
        let (r, c) = keypad(event)?;
        Some(ToneGen { row: ROW_HZ[r], col: COL_HZ[c], sample: 0 })
    }

    pub fn fill(&mut self, out: &mut [i16]) {
        for s in out {
            let t = self.sample as f64 / PCM_RATE as f64;
            let v = TONE_AMPLITUDE * ((TAU * self.row * t).sin() + (TAU * self.col * t).sin());
            *s = v as i16;
            self.sample += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{linear_to_alaw, linear_to_ulaw, alaw_to_linear, ulaw_to_linear};
    use crate::dtmf::{char_to_event, event_to_char};

    fn tone(event: u8, ms: usize) -> Vec<i16> {
        let mut pcm = vec![0; ms * 8];
        ToneGen::new(event).unwrap().fill(&mut pcm);
        pcm
    }

    fn detect(pcm: &[i16]) -> Vec<Detection> {
        let mut d = Detector::default();
        let mut out = Vec::new();
        // Feed in 20 ms packets, as the relay does.
        for chunk in pcm.chunks(160) {
            d.push(chunk, &mut out);
        }
        out.extend(d.reset());
        out
    }

    fn digits(pcm: &[i16]) -> String {
        detect(pcm)
            .iter()
            .filter_map(|d| match d {
                Detection::Start(e) => event_to_char(*e),
                Detection::End(..) => None,
            })
            .collect()
    }

    #[test]
    fn detects_every_digit() {
        for event in 0..16u8 {
            let mut pcm = tone(event, 100);
            pcm.extend(vec![0; 800]);
            let d = detect(&pcm);
            assert_eq!(d[0], Detection::Start(event), "digit {:?}", event_to_char(event));
            let Detection::End(e, ms) = d[1] else { panic!("no end for {event}") };
            assert_eq!(e, event);
            assert!((50..=110).contains(&ms), "duration {ms}");
        }
    }

    #[test]
    fn detects_a_sequence_with_repeats() {
        let mut pcm = Vec::new();
        for c in "1225#".chars() {
            pcm.extend(tone(char_to_event(c).unwrap(), 80));
            pcm.extend(vec![0; 60 * 8]);
        }
        assert_eq!(digits(&pcm), "1225#");
    }

    #[test]
    fn survives_g711() {
        let pcm = tone(char_to_event('8').unwrap(), 100);
        let ulaw: Vec<i16> = pcm.iter().map(|&s| ulaw_to_linear(linear_to_ulaw(s))).collect();
        let alaw: Vec<i16> = pcm.iter().map(|&s| alaw_to_linear(linear_to_alaw(s))).collect();
        assert_eq!(digits(&ulaw), "8");
        assert_eq!(digits(&alaw), "8");
    }

    #[test]
    fn ignores_what_is_not_dtmf() {
        let sine = |hz: f64, amp: f64| -> Vec<i16> {
            (0..8000).map(|i| (amp * (TAU * hz * i as f64 / 8000.0).sin()) as i16).collect()
        };
        assert_eq!(digits(&vec![0; 8000]), "", "silence");
        assert_eq!(digits(&sine(440.0, 12000.0)), "", "one tone");
        assert_eq!(digits(&sine(1209.0, 12000.0)), "", "one DTMF tone alone");
        // Pseudo-random noise.
        let mut x: u32 = 1;
        let noise: Vec<i16> = (0..8000)
            .map(|_| {
                x = x.wrapping_mul(1_103_515_245).wrapping_add(12345);
                ((x >> 16) as i16) / 2
            })
            .collect();
        assert_eq!(digits(&noise), "", "noise");
        // A tone pair buried in loud noise isn't pure enough.
        let mixed: Vec<i16> = tone(5, 1000).iter().zip(&noise).map(|(&t, &n)| t / 4 + n).collect();
        assert_eq!(digits(&mixed), "");
        // Too quiet.
        let quiet: Vec<i16> = tone(5, 200).iter().map(|&s| s / 40).collect();
        assert_eq!(digits(&quiet), "");
    }
}
