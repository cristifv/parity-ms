//! DTMF: RFC 4733 telephone events, in-band tone detection and tone generation.
//! Digits are identified by their RFC 4733 event code: 0–9, `*` (10), `#` (11), A–D (12–15).

pub mod inband;
pub mod rfc4733;

use serde::Serialize;

const KEYS: [char; 16] = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '*', '#', 'A', 'B', 'C', 'D'];
pub const ROW_HZ: [f64; 4] = [697.0, 770.0, 852.0, 941.0];
pub const COL_HZ: [f64; 4] = [1209.0, 1336.0, 1477.0, 1633.0];

pub fn event_to_char(event: u8) -> Option<char> {
    KEYS.get(event as usize).copied()
}

pub fn char_to_event(c: char) -> Option<u8> {
    KEYS.iter().position(|&k| k == c.to_ascii_uppercase()).map(|i| i as u8)
}

/// Keypad position (row, column) of a digit.
pub fn keypad(event: u8) -> Option<(usize, usize)> {
    Some(match event {
        1..=9 => (((event - 1) / 3) as usize, ((event - 1) % 3) as usize),
        0 => (3, 1),
        10 => (3, 0),
        11 => (3, 2),
        12..=15 => ((event - 12) as usize, 3),
        _ => return None,
    })
}

pub fn from_keypad(row: usize, col: usize) -> u8 {
    match (row, col) {
        (3, 0) => 10,
        (3, 1) => 0,
        (3, 2) => 11,
        (r, 3) => 12 + r as u8,
        (r, c) => (r * 3 + c + 1) as u8,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Rfc4733,
    Inband,
}

/// A digit received on a leg, as reported in the `dtmf` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Digit {
    pub digit: char,
    pub duration_ms: u32,
    pub source: Source,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digits_and_keypad_round_trip() {
        for event in 0..16u8 {
            let c = event_to_char(event).unwrap();
            assert_eq!(char_to_event(c), Some(event));
            let (r, col) = keypad(event).unwrap();
            assert_eq!(from_keypad(r, col), event);
        }
        assert_eq!(char_to_event('a'), Some(12));
        assert_eq!(char_to_event('x'), None);
        assert_eq!(keypad(5), Some((1, 1)));
        assert_eq!(keypad(16), None, "flash has no tone");
    }
}
