//! Wall-clock time: Unix milliseconds, RFC 3339 text, and NTP timestamps for RTCP.

use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds between 1900-01-01 (the NTP epoch) and 1970-01-01.
const NTP_UNIX_OFFSET: u64 = 2_208_988_800;

pub fn unix_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// `2026-10-06T09:21:03.123Z`
pub fn rfc3339(unix_ms: u64) -> String {
    let (secs, ms) = (unix_ms / 1000, unix_ms % 1000);
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (y, m, d) = civil_from_days(days as i64);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ms:03}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

/// 64-bit NTP timestamp (seconds since 1900 in the high half, fraction in the low half).
pub fn ntp(unix_ms: u64) -> u64 {
    let secs = unix_ms / 1000 + NTP_UNIX_OFFSET;
    let frac = ((unix_ms % 1000) << 32) / 1000;
    (secs << 32) | frac
}

/// Days since 1970-01-01 to (year, month, day), after Howard Hinnant's algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_rfc3339() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339(951_782_400_000), "2000-02-29T00:00:00.000Z", "leap day");
        assert_eq!(rfc3339(1_791_274_833_947), "2026-10-06T08:20:33.947Z");
    }

    #[test]
    fn ntp_epoch_and_fraction() {
        assert_eq!(ntp(0) >> 32, NTP_UNIX_OFFSET);
        assert_eq!(ntp(500) & 0xFFFF_FFFF, 1 << 31, "half a second");
    }
}
