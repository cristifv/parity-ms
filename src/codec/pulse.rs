//! A codec for tests only: pulse positions (`x-pulse/8000`). Built only with the
//! `test-codec` feature, and accepted only when the server runs with `--test-codec`.
//!
//! Each 20 ms packet lists the frame's pulses, its non-zero samples:
//!
//! ```text
//! count (1 byte), then count × [offset (1 byte, 0–159), amplitude (2 bytes, big-endian)]
//! ```
//!
//! Silence is one byte. Any frame survives encoding and decoding exactly, so a test can
//! send a few impulses and check, to the sample, where and how loud they come out: delay,
//! lost or repeated frames, mixing (pulses from two speakers add up where expected) and
//! recording alignment. Unlike G.711 or Opus, nothing is quantised or smeared.

use super::FRAME_SAMPLES;

/// Encodes one frame: every non-zero sample becomes a pulse.
pub fn encode(pcm: &[i16], payload: &mut Vec<u8>) {
    payload.clear();
    payload.push(0);
    for (offset, &amplitude) in pcm.iter().enumerate().take(FRAME_SAMPLES) {
        if amplitude != 0 {
            payload.push(offset as u8);
            payload.extend_from_slice(&amplitude.to_be_bytes());
        }
    }
    payload[0] = ((payload.len() - 1) / 3) as u8;
}

/// Decodes one packet into a frame of silence with the pulses in place. `None` for a
/// malformed packet.
pub fn decode(payload: &[u8]) -> Option<[i16; FRAME_SAMPLES]> {
    let (&count, pulses) = payload.split_first()?;
    if pulses.len() != count as usize * 3 {
        return None;
    }
    let mut frame = [0i16; FRAME_SAMPLES];
    for p in pulses.chunks_exact(3) {
        let offset = p[0] as usize;
        if offset >= FRAME_SAMPLES {
            return None;
        }
        frame[offset] = i16::from_be_bytes([p[1], p[2]]);
    }
    Some(frame)
}

/// A packet with these pulses (offset, amplitude), for tests to send.
pub fn packet(pulses: &[(u8, i16)]) -> Vec<u8> {
    let mut frame = [0i16; FRAME_SAMPLES];
    for &(offset, amplitude) in pulses {
        frame[offset as usize] = amplitude;
    }
    let mut payload = Vec::new();
    encode(&frame, &mut payload);
    payload
}

/// The pulses in a packet, for tests to check. Empty for silence; `None` if malformed.
pub fn pulses(payload: &[u8]) -> Option<Vec<(u8, i16)>> {
    let frame = decode(payload)?;
    Some(frame.iter().enumerate().filter(|(_, s)| **s != 0).map(|(i, s)| (i as u8, *s)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_come_back_exactly() {
        let mut frame = [0i16; FRAME_SAMPLES];
        frame[0] = 1;
        frame[42] = -32768;
        frame[159] = 32767;
        let mut payload = Vec::new();
        encode(&frame, &mut payload);
        assert_eq!(payload.len(), 1 + 3 * 3);
        assert_eq!(decode(&payload), Some(frame));
        assert_eq!(pulses(&payload), Some(vec![(0, 1), (42, -32768), (159, 32767)]));

        encode(&[0; FRAME_SAMPLES], &mut payload);
        assert_eq!(payload, [0], "silence is one byte");

        let full: Vec<i16> = (1..=FRAME_SAMPLES as i16).collect();
        encode(&full, &mut payload);
        assert_eq!(decode(&payload).unwrap().to_vec(), full, "even a frame with no zeros");
    }

    #[test]
    fn malformed_packets_are_refused() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[1, 5, 0]), None, "truncated");
        assert_eq!(decode(&[1, 160, 0, 1]), None, "offset past the frame");
        assert_eq!(packet(&[(7, 300)]), [1, 7, 1, 44]);
    }
}
