/// Format Unix epoch seconds to PostgreSQL timestamp format (YYYY-MM-DD HH:MM:SS)
/// Uses simple date arithmetic without external dependencies
pub fn format_timestamp(seconds: i64, nanos: u32) -> String {
    const SECONDS_PER_DAY: i64 = 86400;
    const SECONDS_PER_HOUR: i64 = 3600;
    const SECONDS_PER_MINUTE: i64 = 60;

    // Handle negative timestamps by adjusting days and seconds_in_day
    let (days, seconds_in_day) = if seconds >= 0 {
        (seconds / SECONDS_PER_DAY, seconds % SECONDS_PER_DAY)
    } else {
        // For negative: round down days, adjust seconds_in_day to be positive
        let days = (seconds - SECONDS_PER_DAY + 1) / SECONDS_PER_DAY;
        let rem = seconds % SECONDS_PER_DAY;
        let seconds_in_day = if rem < 0 { rem + SECONDS_PER_DAY } else { rem };
        (days, seconds_in_day)
    };

    // Calculate date starting from 1970-01-01
    let mut year = 1970i64;
    let mut month = 1u32;
    let mut day = 1u32;
    let mut remaining_days = days;

    // Handle negative days (before 1970)
    while remaining_days < 0 {
        year -= 1;
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        remaining_days += days_in_year;
    }

    // Add days to get to the correct date
    while remaining_days > 0 {
        let days_in_month = days_in_month(year, month) as i64;
        if remaining_days >= days_in_month {
            remaining_days -= days_in_month;
            month += 1;
            if month > 12 {
                month = 1;
                year += 1;
            }
        } else {
            day += remaining_days as u32;
            remaining_days = 0;
        }
    }

    // Handle day overflow (shouldn't happen with correct logic, but safety check)
    while day > days_in_month(year, month) {
        day -= days_in_month(year, month);
        month += 1;
        if month > 12 {
            month = 1;
            year += 1;
        }
    }

    // Calculate time components
    let hours = (seconds_in_day / SECONDS_PER_HOUR) as u32;
    let minutes = ((seconds_in_day % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE) as u32;
    let secs = (seconds_in_day % SECONDS_PER_MINUTE) as u32;

    // Format fractional seconds (microseconds)
    let micros = nanos / 1000;

    if micros > 0 {
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            year, month, day, hours, minutes, secs, micros
        )
    } else {
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hours, minutes, secs
        )
    }
}

/// Check if a year is a leap year
fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Get number of days in a month
fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Format days since Unix epoch to PostgreSQL date format (YYYY-MM-DD)
pub fn format_date_from_days(days: i32) -> String {
    let seconds = days as i64 * 86400;
    format_timestamp(seconds, 0)[..10].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_timestamp_epoch() {
        // Unix epoch: 1970-01-01 00:00:00
        assert_eq!(format_timestamp(0, 0), "1970-01-01 00:00:00");
    }

    #[test]
    fn test_format_timestamp_with_seconds() {
        // 1000 seconds after epoch: 1970-01-01 00:16:40
        assert_eq!(format_timestamp(1000, 0), "1970-01-01 00:16:40");
    }

    #[test]
    fn test_format_timestamp_with_fractional() {
        // Test with nanoseconds (should convert to microseconds)
        let result = format_timestamp(1736067304, 123456789);
        assert!(result.starts_with("2025-01-05"));
        assert!(result.contains("."));
    }

    #[test]
    fn test_format_date_from_days() {
        // Day 0 = 1970-01-01
        assert_eq!(format_date_from_days(0), "1970-01-01");
        // Day 1 = 1970-01-02
        assert_eq!(format_date_from_days(1), "1970-01-02");
    }
}
