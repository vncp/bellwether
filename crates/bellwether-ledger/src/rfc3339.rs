//! A small, dependency-free RFC 3339 `date-time` validator.
//!
//! Accepts exactly the `date-time` production of RFC 3339 section 5.6:
//! `YYYY-MM-DD` `T` `HH:MM:SS` `[.fraction]` (`Z` | `+HH:MM` | `-HH:MM`).
//! `T` and `Z` may be lowercase, as the RFC permits. Calendar ranges are
//! checked, including leap years; a seconds value of `60` is accepted for
//! leap seconds.

/// Whether `s` is a valid RFC 3339 `date-time`.
#[must_use]
pub fn is_rfc3339(s: &str) -> bool {
    let b = s.as_bytes();
    // Shortest valid form: "YYYY-MM-DDTHH:MM:SSZ" (20 bytes).
    if b.len() < 20 {
        return false;
    }
    let (Some(year), Some(month), Some(day)) = (num(b, 0, 4), num(b, 5, 2), num(b, 8, 2)) else {
        return false;
    };
    if b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't') {
        return false;
    }
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return false;
    }
    let (Some(hour), Some(minute), Some(second)) = (num(b, 11, 2), num(b, 14, 2), num(b, 17, 2))
    else {
        return false;
    };
    if b[13] != b':' || b[16] != b':' || hour > 23 || minute > 59 || second > 60 {
        return false;
    }
    let mut i = 19;
    if b[i] == b'.' {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    match &b[i..] {
        [b'Z' | b'z'] => true,
        [b'+' | b'-', ..] if b.len() - i == 6 => {
            let (Some(oh), Some(om)) = (num(b, i + 1, 2), num(b, i + 4, 2)) else {
                return false;
            };
            b[i + 3] == b':' && oh <= 23 && om <= 59
        }
        _ => false,
    }
}

/// Parses `len` ASCII digits starting at `at`.
fn num(b: &[u8], at: usize, len: usize) -> Option<u32> {
    let digits = b.get(at..at + len)?;
    digits.iter().try_fold(0u32, |acc, &c| {
        c.is_ascii_digit().then(|| acc * 10 + u32::from(c - b'0'))
    })
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        2 if (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400) => {
            29
        }
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

#[cfg(test)]
mod tests {
    use super::is_rfc3339;

    #[test]
    fn accepts_valid_forms() {
        for s in [
            "2026-10-24T00:00:00Z",
            "2026-10-24t00:00:00z",
            "2026-10-24T23:59:59.123456789Z",
            "2026-10-24T12:00:00+05:30",
            "2026-10-24T12:00:00-00:00",
            "2024-02-29T00:00:00Z",
            "2016-12-31T23:59:60Z",
            "2000-02-29T00:00:00Z",
        ] {
            assert!(is_rfc3339(s), "{s} should be accepted");
        }
    }

    #[test]
    fn rejects_invalid_forms() {
        for s in [
            "",
            "yesterday",
            "2026-10-24",
            "2026-10-24 00:00:00Z",
            "2026-10-24T00:00:00",
            "2026-10-24T00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-00-01T00:00:00Z",
            "2026-04-31T00:00:00Z",
            "2025-02-29T00:00:00Z",
            "1900-02-29T00:00:00Z",
            "2026-10-24T24:00:00Z",
            "2026-10-24T00:60:00Z",
            "2026-10-24T00:00:61Z",
            "2026-10-24T00:00:00.Z",
            "2026-10-24T00:00:00+0530",
            "2026-10-24T00:00:00+24:00",
            "2026-10-24T00:00:00+05:60",
            "2026-10-24T00:00:00Zjunk",
            "2026-1a-24T00:00:00Z",
            "+026-10-24T00:00:00Z",
            "2026-10-24T00:00:00+05:30:00",
        ] {
            assert!(!is_rfc3339(s), "{s:?} should be rejected");
        }
    }
}
