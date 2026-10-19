//! STUN (RFC 5389), as much as ICE-lite needs: Binding requests in, Binding success
//! responses out, with MESSAGE-INTEGRITY (HMAC-SHA1 with the ICE password) and FINGERPRINT
//! (CRC-32).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::sign::Signer;

pub const MAGIC_COOKIE: u32 = 0x2112_A442;
const HEADER_LEN: usize = 20;

pub const BINDING_REQUEST: u16 = 0x0001;
pub const BINDING_SUCCESS: u16 = 0x0101;

pub const USERNAME: u16 = 0x0006;
pub const MESSAGE_INTEGRITY: u16 = 0x0008;
pub const XOR_MAPPED_ADDRESS: u16 = 0x0020;
pub const PRIORITY: u16 = 0x0024;
pub const USE_CANDIDATE: u16 = 0x0025;
pub const FINGERPRINT: u16 = 0x8028;
pub const ICE_CONTROLLED: u16 = 0x8029;
pub const ICE_CONTROLLING: u16 = 0x802A;

const FINGERPRINT_XOR: u32 = 0x5354_554E;

/// A STUN message starts with two zero bits and carries the magic cookie (RFC 7983 tells
/// STUN from DTLS and RTP on one port by the first byte: 0–3).
pub fn is_stun(packet: &[u8]) -> bool {
    packet.len() >= HEADER_LEN
        && packet[0] < 4
        && u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]) == MAGIC_COOKIE
}

/// A parsed STUN message, borrowing the packet.
#[derive(Debug)]
pub struct Message<'a> {
    pub kind: u16,
    pub transaction: [u8; 12],
    raw: &'a [u8],
    /// (type, value, offset of the attribute's header)
    attrs: Vec<(u16, &'a [u8], usize)>,
}

impl<'a> Message<'a> {
    pub fn parse(raw: &'a [u8]) -> Option<Message<'a>> {
        if !is_stun(raw) {
            return None;
        }
        let kind = u16::from_be_bytes([raw[0], raw[1]]);
        let len = u16::from_be_bytes([raw[2], raw[3]]) as usize;
        if !len.is_multiple_of(4) || HEADER_LEN + len != raw.len() {
            return None;
        }
        let transaction: [u8; 12] = raw[8..20].try_into().ok()?;
        let mut attrs = Vec::new();
        let mut at = HEADER_LEN;
        while at + 4 <= raw.len() {
            let t = u16::from_be_bytes([raw[at], raw[at + 1]]);
            let l = u16::from_be_bytes([raw[at + 2], raw[at + 3]]) as usize;
            let value = raw.get(at + 4..at + 4 + l)?;
            attrs.push((t, value, at));
            at += 4 + l.div_ceil(4) * 4;
        }
        (at == raw.len()).then_some(Message { kind, transaction, raw, attrs })
    }

    pub fn attr(&self, t: u16) -> Option<&'a [u8]> {
        self.attrs.iter().find(|(kind, _, _)| *kind == t).map(|(_, v, _)| *v)
    }

    pub fn has(&self, t: u16) -> bool {
        self.attr(t).is_some()
    }

    pub fn username(&self) -> Option<&'a str> {
        std::str::from_utf8(self.attr(USERNAME)?).ok()
    }

    /// MESSAGE-INTEGRITY checks out with `key` (the ICE password). Attributes after it
    /// other than FINGERPRINT are not allowed.
    pub fn check_integrity(&self, key: &[u8]) -> bool {
        let Some(i) = self.attrs.iter().position(|(t, _, _)| *t == MESSAGE_INTEGRITY) else { return false };
        if self.attrs[i + 1..].iter().any(|(t, _, _)| *t != FINGERPRINT) {
            return false;
        }
        let (_, value, offset) = self.attrs[i];
        let Some(mac) = hmac_sha1(key, &with_length(self.raw, offset, offset + 24 - HEADER_LEN)) else { return false };
        value.len() == 20 && openssl::memcmp::eq(value, &mac)
    }

    /// FINGERPRINT is present, last, and right.
    pub fn check_fingerprint(&self) -> bool {
        let Some(&(FINGERPRINT, value, offset)) = self.attrs.last() else { return false };
        value.len() == 4
            && u32::from_be_bytes(value.try_into().unwrap())
                == crc32(&with_length(self.raw, offset, offset + 8 - HEADER_LEN)) ^ FINGERPRINT_XOR
    }

    pub fn xor_mapped_address(&self) -> Option<SocketAddr> {
        decode_xor_address(self.attr(XOR_MAPPED_ADDRESS)?, &self.transaction)
    }
}

/// The message's first `end` bytes, with the header's length set to `length` (as STUN
/// computes MESSAGE-INTEGRITY and FINGERPRINT).
fn with_length(raw: &[u8], end: usize, length: usize) -> Vec<u8> {
    let mut m = raw[..end].to_vec();
    m[2..4].copy_from_slice(&(length as u16).to_be_bytes());
    m
}

/// Builds a STUN message.
pub struct Builder {
    buf: Vec<u8>,
}

impl Builder {
    pub fn new(kind: u16, transaction: [u8; 12]) -> Builder {
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(&kind.to_be_bytes());
        buf.extend_from_slice(&[0, 0]);
        buf.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
        buf.extend_from_slice(&transaction);
        Builder { buf }
    }

    pub fn attr(mut self, t: u16, value: &[u8]) -> Builder {
        self.buf.extend_from_slice(&t.to_be_bytes());
        self.buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(value);
        self.buf.resize(self.buf.len().div_ceil(4) * 4, 0);
        self.set_length();
        self
    }

    pub fn xor_mapped_address(self, addr: SocketAddr) -> Builder {
        let transaction: [u8; 12] = self.buf[8..20].try_into().unwrap();
        let value = encode_xor_address(addr, &transaction);
        self.attr(XOR_MAPPED_ADDRESS, &value)
    }

    /// Appends MESSAGE-INTEGRITY computed with `key`.
    pub fn integrity(mut self, key: &[u8]) -> Builder {
        let end = self.buf.len();
        let mac = hmac_sha1(key, &with_length(&self.buf, end, end + 24 - HEADER_LEN)).expect("HMAC-SHA1");
        self.buf.extend_from_slice(&MESSAGE_INTEGRITY.to_be_bytes());
        self.buf.extend_from_slice(&20u16.to_be_bytes());
        self.buf.extend_from_slice(&mac);
        self.set_length();
        self
    }

    /// Appends FINGERPRINT and returns the message.
    pub fn fingerprint(mut self) -> Vec<u8> {
        let end = self.buf.len();
        let crc = crc32(&with_length(&self.buf, end, end + 8 - HEADER_LEN)) ^ FINGERPRINT_XOR;
        self.buf.extend_from_slice(&FINGERPRINT.to_be_bytes());
        self.buf.extend_from_slice(&4u16.to_be_bytes());
        self.buf.extend_from_slice(&crc.to_be_bytes());
        self.set_length();
        self.buf
    }

    /// The message as it is, without FINGERPRINT.
    pub fn build(self) -> Vec<u8> {
        self.buf
    }

    fn set_length(&mut self) {
        let len = (self.buf.len() - HEADER_LEN) as u16;
        self.buf[2..4].copy_from_slice(&len.to_be_bytes());
    }
}

fn encode_xor_address(addr: SocketAddr, transaction: &[u8; 12]) -> Vec<u8> {
    let port = addr.port() ^ (MAGIC_COOKIE >> 16) as u16;
    let mut v = vec![0];
    match addr.ip() {
        IpAddr::V4(ip) => {
            v.push(0x01);
            v.extend_from_slice(&port.to_be_bytes());
            v.extend_from_slice(&(u32::from(ip) ^ MAGIC_COOKIE).to_be_bytes());
        }
        IpAddr::V6(ip) => {
            v.push(0x02);
            v.extend_from_slice(&port.to_be_bytes());
            let mask = xor_mask_v6(transaction);
            v.extend(ip.octets().iter().zip(mask).map(|(a, m)| a ^ m));
        }
    }
    v
}

fn decode_xor_address(v: &[u8], transaction: &[u8; 12]) -> Option<SocketAddr> {
    let port = u16::from_be_bytes([*v.get(2)?, *v.get(3)?]) ^ (MAGIC_COOKIE >> 16) as u16;
    let ip = match *v.get(1)? {
        0x01 => IpAddr::V4(Ipv4Addr::from(u32::from_be_bytes(v.get(4..8)?.try_into().ok()?) ^ MAGIC_COOKIE)),
        0x02 => {
            let mask = xor_mask_v6(transaction);
            let bytes: [u8; 16] = v.get(4..20)?.iter().zip(mask).map(|(a, m)| a ^ m).collect::<Vec<_>>().try_into().ok()?;
            IpAddr::V6(Ipv6Addr::from(bytes))
        }
        _ => return None,
    };
    Some(SocketAddr::new(ip, port))
}

fn xor_mask_v6(transaction: &[u8; 12]) -> [u8; 16] {
    let mut mask = [0u8; 16];
    mask[..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
    mask[4..].copy_from_slice(transaction);
    mask
}

fn hmac_sha1(key: &[u8], data: &[u8]) -> Option<Vec<u8>> {
    let key = PKey::hmac(key).ok()?;
    let mut signer = Signer::new(MessageDigest::sha1(), &key).ok()?;
    signer.update(data).ok()?;
    signer.sign_to_vec().ok()
}

/// CRC-32 (IEEE 802.3), as FINGERPRINT uses it.
pub fn crc32(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
                k += 1;
            }
            table[i] = c;
            i += 1;
        }
        table
    };
    !data.iter().fold(!0u32, |crc, &b| TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace().map(|b| u8::from_str_radix(b, 16).unwrap()).collect()
    }

    /// RFC 5769 §2.1.
    fn sample_request() -> Vec<u8> {
        hex("00 01 00 58 21 12 a4 42 b7 e7 a7 01 bc 34 d6 86 fa 87 df ae
             80 22 00 10 53 54 55 4e 20 74 65 73 74 20 63 6c 69 65 6e 74
             00 24 00 04 6e 00 01 ff 80 29 00 08 93 2f f9 b1 51 26 3b 36
             00 06 00 09 65 76 74 6a 3a 68 36 76 59 20 20 20
             00 08 00 14 9a ea a7 0c bf d8 cb 56 78 1e f2 b5 b2 d3 f2 49 c1 b5 71 a2
             80 28 00 04 e5 7a 3b cf")
    }

    /// RFC 5769 §2.2.
    fn sample_response() -> Vec<u8> {
        hex("01 01 00 3c 21 12 a4 42 b7 e7 a7 01 bc 34 d6 86 fa 87 df ae
             80 22 00 0b 74 65 73 74 20 76 65 63 74 6f 72 20
             00 20 00 08 00 01 a1 47 e1 12 a6 43
             00 08 00 14 2b 91 f5 99 fd 9e 90 c3 8c 74 89 f9 2a f9 ba 53 f0 6b e7 d7
             80 28 00 04 c0 7d 4c 96")
    }

    const PASSWORD: &[u8] = b"VOkJxbRl1RmTxUk/WvJxBt";

    #[test]
    fn rfc5769_request_checks_out() {
        let raw = sample_request();
        let m = Message::parse(&raw).unwrap();
        assert_eq!(m.kind, BINDING_REQUEST);
        assert_eq!(m.username(), Some("evtj:h6vY"));
        assert!(m.has(PRIORITY) && m.has(ICE_CONTROLLED) && !m.has(USE_CANDIDATE));
        assert!(m.check_fingerprint());
        assert!(m.check_integrity(PASSWORD));
        assert!(!m.check_integrity(b"wrong password"));
    }

    #[test]
    fn rfc5769_response_checks_out() {
        let raw = sample_response();
        let m = Message::parse(&raw).unwrap();
        assert_eq!(m.kind, BINDING_SUCCESS);
        assert_eq!(m.xor_mapped_address(), Some("192.0.2.1:32853".parse().unwrap()));
        assert!(m.check_fingerprint());
        assert!(m.check_integrity(PASSWORD));
    }

    #[test]
    fn tampering_is_caught() {
        let mut raw = sample_request();
        raw[30] ^= 1; // inside SOFTWARE
        let m = Message::parse(&raw).unwrap();
        assert!(!m.check_fingerprint());
        assert!(!m.check_integrity(PASSWORD));
    }

    #[test]
    fn built_responses_parse_and_check_out() {
        let tx = [7u8; 12];
        for addr in ["203.0.113.5:40000", "[2001:db8::1]:5004"] {
            let addr: SocketAddr = addr.parse().unwrap();
            let raw = Builder::new(BINDING_SUCCESS, tx).xor_mapped_address(addr).integrity(PASSWORD).fingerprint();
            let m = Message::parse(&raw).unwrap();
            assert_eq!((m.kind, m.transaction), (BINDING_SUCCESS, tx));
            assert_eq!(m.xor_mapped_address(), Some(addr));
            assert!(m.check_integrity(PASSWORD) && m.check_fingerprint());
        }
    }

    #[test]
    fn stun_is_told_apart_from_rtp_and_dtls() {
        assert!(is_stun(&sample_request()));
        let mut rtp = vec![0x80, 0, 0, 1];
        rtp.resize(40, 0);
        assert!(!is_stun(&rtp));
        assert!(!is_stun(&[22, 254, 253, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]), "DTLS");
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926, "the standard CRC-32 check value");
    }
}
