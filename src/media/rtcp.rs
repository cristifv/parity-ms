//! RTCP Sender Reports (RFC 3550 §6.4.1) for the streams this server generates. An RTP
//! timestamp is a media clock with a random start; the report ties it to wall-clock (NTP)
//! time, so a receiver knows when each packet's audio was sent. It also counts the
//! packets and bytes sent. Each report is a compound packet with the mandatory SDES CNAME.

use crate::clock::ntp;

pub const SR: u8 = 200;
pub const SDES: u8 = 202;
const CNAME: u8 = 1;

pub struct SenderInfo {
    pub ssrc: u32,
    /// Wall-clock time of the report, Unix milliseconds.
    pub at_ms: u64,
    /// RTP timestamp matching `at_ms`, on the stream's clock.
    pub rtp_timestamp: u32,
    pub packets: u32,
    pub octets: u32,
}

/// SR + SDES (CNAME), as one compound RTCP packet.
pub fn sender_report(info: &SenderInfo, cname: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(64);
    // SR: V=2, no padding, no report blocks; 7 words, so length 6.
    p.extend_from_slice(&[0x80, SR, 0, 6]);
    p.extend_from_slice(&info.ssrc.to_be_bytes());
    p.extend_from_slice(&ntp(info.at_ms).to_be_bytes());
    p.extend_from_slice(&info.rtp_timestamp.to_be_bytes());
    p.extend_from_slice(&info.packets.to_be_bytes());
    p.extend_from_slice(&info.octets.to_be_bytes());

    // SDES: one chunk with CNAME, ended by a null item and padded to 32 bits.
    let name = &cname.as_bytes()[..cname.len().min(255)];
    let start = p.len();
    p.extend_from_slice(&[0x81, SDES, 0, 0]);
    p.extend_from_slice(&info.ssrc.to_be_bytes());
    p.push(CNAME);
    p.push(name.len() as u8);
    p.extend_from_slice(name);
    p.push(0);
    while (p.len() - start) % 4 != 0 {
        p.push(0);
    }
    let words = ((p.len() - start) / 4 - 1) as u16;
    p[start + 2..start + 4].copy_from_slice(&words.to_be_bytes());
    p
}

/// The sender info of an SR at the start of `packet`, if it is one.
pub fn parse_sender_report(packet: &[u8]) -> Option<SenderInfoRaw> {
    if packet.len() < 28 || packet[0] >> 6 != 2 || packet[1] != SR {
        return None;
    }
    let word = |i: usize| u32::from_be_bytes(packet[i..i + 4].try_into().unwrap());
    Some(SenderInfoRaw {
        ssrc: word(4),
        ntp: ((word(8) as u64) << 32) | word(12) as u64,
        rtp_timestamp: word(16),
        packets: word(20),
        octets: word(24),
    })
}

#[derive(Debug, PartialEq, Eq)]
pub struct SenderInfoRaw {
    pub ssrc: u32,
    pub ntp: u64,
    pub rtp_timestamp: u32,
    pub packets: u32,
    pub octets: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_a_compound_sr_with_cname() {
        let info = SenderInfo { ssrc: 0xABCD, at_ms: 1_791_274_833_500, rtp_timestamp: 4242, packets: 10, octets: 1600 };
        let p = sender_report(&info, "parity-ms");
        assert_eq!(p.len() % 4, 0);
        let sr = parse_sender_report(&p).unwrap();
        assert_eq!(sr, SenderInfoRaw { ssrc: 0xABCD, ntp: ntp(info.at_ms), rtp_timestamp: 4242, packets: 10, octets: 1600 });
        assert_eq!((sr.ntp >> 32) - 2_208_988_800, 1_791_274_833, "NTP seconds since 1900");
        // The SDES that follows.
        let sdes = &p[28..];
        assert_eq!((sdes[0], sdes[1]), (0x81, SDES));
        let words = u16::from_be_bytes([sdes[2], sdes[3]]) as usize;
        assert_eq!(sdes.len(), (words + 1) * 4);
        assert_eq!((sdes[8], sdes[9] as usize), (CNAME, "parity-ms".len()));
        assert_eq!(&sdes[10..19], b"parity-ms");
    }
}
