//! Calendar helpers for date/epoch-day conversions.
//!
//! Used by flush (day directory naming) and retention (date directory parsing).

/// Convert days since Unix epoch to (year, month, day).
pub(crate) fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Civil days algorithm
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Convert (year, month, day) to days since Unix epoch.
pub(crate) fn ymd_to_days(year: u64, month: u64, day: u64) -> u64 {
    let y = if month <= 2 { year - 1 } else { year };
    let m = if month <= 2 { month + 9 } else { month - 3 };
    let era = y / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_to_ymd_known_dates() {
        // 2026-03-15 is day 20527 since Unix epoch
        let (y, m, d) = days_to_ymd(20527);
        assert_eq!(y, 2026);
        assert_eq!(m, 3);
        assert_eq!(d, 15);

        // 1970-01-01 is day 0
        let (y, m, d) = days_to_ymd(0);
        assert_eq!(y, 1970);
        assert_eq!(m, 1);
        assert_eq!(d, 1);
    }

    #[test]
    fn ymd_round_trip() {
        assert_eq!(ymd_to_days(1970, 1, 1), 0);
        assert_eq!(ymd_to_days(2026, 3, 15), 20527);
    }

    #[test]
    fn round_trip_consistency() {
        for day in [0, 365, 730, 10000, 20527, 30000] {
            let (y, m, d) = days_to_ymd(day);
            assert_eq!(ymd_to_days(y, m, d), day, "round-trip failed for day {day}");
        }
    }
}
