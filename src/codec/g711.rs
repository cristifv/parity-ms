//! G.711 μ-law and A-law (ITU-T G.711), after the reference Sun implementation.

const SEG_A_END: [i32; 8] = [0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF];
const SEG_U_END: [i32; 8] = [0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF];
const U_BIAS: i32 = 0x84;
const U_CLIP: i32 = 8159;

fn segment(value: i32, table: &[i32; 8]) -> usize {
    table.iter().position(|&end| value <= end).unwrap_or(8)
}

pub fn linear_to_ulaw(sample: i16) -> u8 {
    let mut v = (sample as i32) >> 2;
    let mask = if v < 0 {
        v = -v;
        0x7F
    } else {
        0xFF
    };
    v = v.min(U_CLIP) + (U_BIAS >> 2);
    let seg = segment(v, &SEG_U_END);
    let code = if seg >= 8 { 0x7F } else { ((seg as i32) << 4) | ((v >> (seg + 1)) & 0x0F) };
    (code ^ mask) as u8
}

pub fn ulaw_to_linear(code: u8) -> i16 {
    let u = !code as i32;
    let t = (((u & 0x0F) << 3) + U_BIAS) << ((u & 0x70) >> 4);
    (if u & 0x80 != 0 { U_BIAS - t } else { t - U_BIAS }) as i16
}

pub fn linear_to_alaw(sample: i16) -> u8 {
    let mut v = (sample as i32) >> 3;
    let mask = if v >= 0 {
        0xD5
    } else {
        v = -v - 1;
        0x55
    };
    let seg = segment(v, &SEG_A_END);
    let code = if seg >= 8 {
        0x7F
    } else {
        let quant = if seg < 2 { (v >> 1) & 0x0F } else { (v >> seg) & 0x0F };
        ((seg as i32) << 4) | quant
    };
    (code ^ mask) as u8
}

pub fn alaw_to_linear(code: u8) -> i16 {
    let a = (code ^ 0x55) as i32;
    let mut t = (a & 0x0F) << 4;
    match (a & 0x70) >> 4 {
        0 => t += 8,
        1 => t += 0x108,
        seg => t = (t + 0x108) << (seg - 1),
    }
    (if a & 0x80 != 0 { t } else { -t }) as i16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_codes_match_the_standard() {
        assert_eq!(linear_to_ulaw(0), 0xFF);
        assert_eq!(ulaw_to_linear(0xFF), 0);
        assert_eq!(linear_to_alaw(0), 0xD5);
        assert_eq!(alaw_to_linear(0xD5), 8);
    }

    #[test]
    fn every_code_survives_decode_then_encode() {
        for code in 0..=255u8 {
            assert_eq!(linear_to_alaw(alaw_to_linear(code)), code, "A-law {code:#04x}");
            // μ-law has two codes for zero (0x7F and 0xFF); both decode to 0.
            if code != 0x7F {
                assert_eq!(linear_to_ulaw(ulaw_to_linear(code)), code, "μ-law {code:#04x}");
            }
        }
    }

    #[test]
    fn quantisation_error_stays_within_the_segment_step() {
        for s in (i16::MIN..=i16::MAX).step_by(97) {
            let u = ulaw_to_linear(linear_to_ulaw(s)) as i32;
            let a = alaw_to_linear(linear_to_alaw(s)) as i32;
            // Logarithmic quantisation: the error grows with the magnitude, about 1/32 of it.
            let tolerance = (s as i32).abs() / 16 + 16;
            assert!((u - s as i32).abs() <= tolerance, "μ-law {s} -> {u}");
            assert!((a - s as i32).abs() <= tolerance, "A-law {s} -> {a}");
        }
    }
}
