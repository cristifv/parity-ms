//! Minimal RTP header handling (RFC 3550).

use std::ops::Range;

pub const HEADER_LEN: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub marker: bool,
    pub payload_type: u8,
    pub sequence: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    /// Where the payload sits in the packet, after CSRCs and header extension, before padding.
    pub payload: Range<usize>,
}

/// Parses an RTP packet. Returns `None` for anything that isn't well-formed RTP version 2.
pub fn parse(p: &[u8]) -> Option<Header> {
    if p.len() < HEADER_LEN || p[0] >> 6 != 2 {
        return None;
    }
    let csrc_count = (p[0] & 0x0F) as usize;
    let mut start = HEADER_LEN + 4 * csrc_count;
    if p[0] & 0x10 != 0 {
        let ext = p.get(start..start + 4)?;
        start += 4 + 4 * u16::from_be_bytes([ext[2], ext[3]]) as usize;
    }
    let mut end = p.len();
    if p[0] & 0x20 != 0 {
        let padding = *p.last()? as usize;
        if padding == 0 {
            return None;
        }
        end = end.checked_sub(padding)?;
    }
    if start > end {
        return None;
    }
    Some(Header {
        marker: p[1] & 0x80 != 0,
        payload_type: p[1] & 0x7F,
        sequence: u16::from_be_bytes([p[2], p[3]]),
        timestamp: u32::from_be_bytes([p[4], p[5], p[6], p[7]]),
        ssrc: u32::from_be_bytes([p[8], p[9], p[10], p[11]]),
        payload: start..end,
    })
}

pub fn set_payload_type(p: &mut [u8], payload_type: u8) {
    p[1] = (p[1] & 0x80) | (payload_type & 0x7F);
}

/// Appends a packet with a plain 12-byte header to `out`.
pub fn write(out: &mut Vec<u8>, marker: bool, payload_type: u8, sequence: u16, timestamp: u32, ssrc: u32, payload: &[u8]) {
    out.reserve(HEADER_LEN + payload.len());
    out.push(0x80);
    out.push(((marker as u8) << 7) | (payload_type & 0x7F));
    out.extend_from_slice(&sequence.to_be_bytes());
    out.extend_from_slice(&timestamp.to_be_bytes());
    out.extend_from_slice(&ssrc.to_be_bytes());
    out.extend_from_slice(payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_parse() {
        let mut p = Vec::new();
        write(&mut p, true, 111, 65535, 0xDEADBEEF, 42, b"hello");
        let h = parse(&p).unwrap();
        assert!(h.marker);
        assert_eq!((h.payload_type, h.sequence, h.timestamp, h.ssrc), (111, 65535, 0xDEADBEEF, 42));
        assert_eq!(&p[h.payload], b"hello");
    }

    #[test]
    fn skips_csrcs_extension_and_padding() {
        let mut p = vec![0x80 | 0x20 | 0x10 | 1, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3];
        p.extend_from_slice(&[9, 9, 9, 9]); // one CSRC
        p.extend_from_slice(&[0xBE, 0xDE, 0, 1, 7, 7, 7, 7]); // extension, one word
        p.extend_from_slice(b"pay");
        p.extend_from_slice(&[0, 0, 3]); // three bytes of padding
        let h = parse(&p).unwrap();
        assert_eq!(&p[h.payload], b"pay");
    }

    #[test]
    fn rejects_junk() {
        assert!(parse(b"short").is_none());
        assert!(parse(&[0x40; 12]).is_none(), "version 1");
        let mut bad_padding = vec![0xA0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        bad_padding.push(200);
        assert!(parse(&bad_padding).is_none());
    }

    #[test]
    fn rewrites_payload_type_and_keeps_marker() {
        let mut p = Vec::new();
        write(&mut p, true, 0, 1, 2, 3, b"x");
        set_payload_type(&mut p, 96);
        let h = parse(&p).unwrap();
        assert_eq!((h.marker, h.payload_type), (true, 96));
    }
}
