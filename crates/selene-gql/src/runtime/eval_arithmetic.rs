//! Arithmetic, negation, CAST, and duration parsing.

use smol_str::SmolStr;

use crate::ast::expr::*;
use crate::runtime::eval::EvalContext;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── Arithmetic ─────────────────────────────────────────────────────

pub(crate) fn eval_arithmetic(
    left: &GqlValue,
    op: ArithOp,
    right: &GqlValue,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    // NULL propagation
    if left.is_null() || right.is_null() {
        return Ok(GqlValue::Null);
    }

    // Temporal arithmetic (spec §4.16.6.4, §20.26, §20.28)
    // All operations use checked arithmetic to prevent i64 overflow
    let temporal_overflow = || GqlError::type_error("temporal arithmetic overflow");
    match (left, op, right) {
        // datetime - datetime → duration
        (GqlValue::ZonedDateTime(a), ArithOp::Sub, GqlValue::ZonedDateTime(b)) => {
            return Ok(GqlValue::Duration(
                crate::types::value::GqlDuration::day_time(
                    a.nanos.checked_sub(b.nanos).ok_or_else(temporal_overflow)?,
                ),
            ));
        }
        (GqlValue::LocalDateTime(a), ArithOp::Sub, GqlValue::LocalDateTime(b)) => {
            return Ok(GqlValue::Duration(
                crate::types::value::GqlDuration::day_time(
                    a.nanos.checked_sub(b.nanos).ok_or_else(temporal_overflow)?,
                ),
            ));
        }
        // datetime ± duration → datetime
        (GqlValue::ZonedDateTime(dt), ArithOp::Add, GqlValue::Duration(d)) => {
            return Ok(GqlValue::ZonedDateTime(
                crate::types::value::ZonedDateTime {
                    nanos: dt
                        .nanos
                        .checked_add(d.nanos)
                        .ok_or_else(temporal_overflow)?,
                    offset_seconds: dt.offset_seconds,
                },
            ));
        }
        (GqlValue::ZonedDateTime(dt), ArithOp::Sub, GqlValue::Duration(d)) => {
            return Ok(GqlValue::ZonedDateTime(
                crate::types::value::ZonedDateTime {
                    nanos: dt
                        .nanos
                        .checked_sub(d.nanos)
                        .ok_or_else(temporal_overflow)?,
                    offset_seconds: dt.offset_seconds,
                },
            ));
        }
        (GqlValue::LocalDateTime(dt), ArithOp::Add, GqlValue::Duration(d)) => {
            return Ok(GqlValue::LocalDateTime(
                crate::types::value::GqlLocalDateTime {
                    nanos: dt
                        .nanos
                        .checked_add(d.nanos)
                        .ok_or_else(temporal_overflow)?,
                },
            ));
        }
        (GqlValue::LocalDateTime(dt), ArithOp::Sub, GqlValue::Duration(d)) => {
            return Ok(GqlValue::LocalDateTime(
                crate::types::value::GqlLocalDateTime {
                    nanos: dt
                        .nanos
                        .checked_sub(d.nanos)
                        .ok_or_else(temporal_overflow)?,
                },
            ));
        }
        // duration + datetime → datetime (commutative)
        (GqlValue::Duration(d), ArithOp::Add, GqlValue::ZonedDateTime(dt)) => {
            return Ok(GqlValue::ZonedDateTime(
                crate::types::value::ZonedDateTime {
                    nanos: dt
                        .nanos
                        .checked_add(d.nanos)
                        .ok_or_else(temporal_overflow)?,
                    offset_seconds: dt.offset_seconds,
                },
            ));
        }
        (GqlValue::Duration(d), ArithOp::Add, GqlValue::LocalDateTime(dt)) => {
            return Ok(GqlValue::LocalDateTime(
                crate::types::value::GqlLocalDateTime {
                    nanos: dt
                        .nanos
                        .checked_add(d.nanos)
                        .ok_or_else(temporal_overflow)?,
                },
            ));
        }
        // duration ± duration → duration
        (GqlValue::Duration(a), ArithOp::Add, GqlValue::Duration(b)) => {
            return Ok(GqlValue::Duration(
                crate::types::value::GqlDuration::day_time(
                    a.nanos.checked_add(b.nanos).ok_or_else(temporal_overflow)?,
                ),
            ));
        }
        (GqlValue::Duration(a), ArithOp::Sub, GqlValue::Duration(b)) => {
            return Ok(GqlValue::Duration(
                crate::types::value::GqlDuration::day_time(
                    a.nanos.checked_sub(b.nanos).ok_or_else(temporal_overflow)?,
                ),
            ));
        }
        // duration * number → duration
        (GqlValue::Duration(d), ArithOp::Mul, n)
            if matches!(n, GqlValue::Int(_) | GqlValue::UInt(_) | GqlValue::Float(_)) =>
        {
            let factor = n.as_float()?;
            let result = d.nanos as f64 * factor;
            if !result.is_finite() || result > i64::MAX as f64 || result < i64::MIN as f64 {
                return Err(GqlError::type_error("duration arithmetic overflow"));
            }
            return Ok(GqlValue::Duration(
                crate::types::value::GqlDuration::day_time(result as i64),
            ));
        }
        (n, ArithOp::Mul, GqlValue::Duration(d))
            if matches!(n, GqlValue::Int(_) | GqlValue::UInt(_) | GqlValue::Float(_)) =>
        {
            let factor = n.as_float()?;
            let result = d.nanos as f64 * factor;
            if !result.is_finite() || result > i64::MAX as f64 || result < i64::MIN as f64 {
                return Err(GqlError::type_error("duration arithmetic overflow"));
            }
            return Ok(GqlValue::Duration(
                crate::types::value::GqlDuration::day_time(result as i64),
            ));
        }
        // duration / number → duration
        (GqlValue::Duration(d), ArithOp::Div, n)
            if matches!(n, GqlValue::Int(_) | GqlValue::UInt(_) | GqlValue::Float(_)) =>
        {
            let divisor = n.as_float()?;
            if divisor == 0.0 {
                return Err(GqlError::type_error("division by zero"));
            }
            let result = d.nanos as f64 / divisor;
            if !result.is_finite() || result > i64::MAX as f64 || result < i64::MIN as f64 {
                return Err(GqlError::type_error("duration arithmetic overflow"));
            }
            return Ok(GqlValue::Duration(
                crate::types::value::GqlDuration::day_time(result as i64),
            ));
        }
        // Date ± duration → Date (add/subtract days)
        (GqlValue::Date(d), ArithOp::Add, GqlValue::Duration(dur)) => {
            // Use div_euclid so negative sub-day durations round toward previous day
            let day_delta = dur.nanos.div_euclid(86400 * 1_000_000_000) as i32;
            let days = d
                .days
                .checked_add(day_delta)
                .ok_or_else(|| GqlError::type_error("date arithmetic overflow"))?;
            return Ok(GqlValue::Date(crate::types::value::GqlDate { days }));
        }
        (GqlValue::Date(d), ArithOp::Sub, GqlValue::Duration(dur)) => {
            let day_delta = dur.nanos.div_euclid(86400 * 1_000_000_000) as i32;
            let days = d
                .days
                .checked_sub(day_delta)
                .ok_or_else(|| GqlError::type_error("date arithmetic overflow"))?;
            return Ok(GqlValue::Date(crate::types::value::GqlDate { days }));
        }
        // Date - Date → duration (days)
        (GqlValue::Date(a), ArithOp::Sub, GqlValue::Date(b)) => {
            let nanos = (i64::from(a.days) - i64::from(b.days)) * 86400 * 1_000_000_000;
            return Ok(GqlValue::Duration(
                crate::types::value::GqlDuration::day_time(nanos),
            ));
        }
        _ => {} // fall through to numeric arithmetic
    }

    // String concatenation: string + anything or anything + string
    let left_is_str = matches!(left, GqlValue::String(_));
    let right_is_str = matches!(right, GqlValue::String(_));
    if (left_is_str || right_is_str) && op == ArithOp::Add {
        let l = super::eval::value_to_string(left);
        let r = super::eval::value_to_string(right);
        return Ok(GqlValue::String(smol_str::SmolStr::new(l + &r)));
    }

    // Float promotion (rule 1)
    if matches!(left, GqlValue::Float(_)) || matches!(right, GqlValue::Float(_)) {
        let l = left.as_float()?;
        let r = right.as_float()?;
        let result = match op {
            ArithOp::Add => l + r,
            ArithOp::Sub => l - r,
            ArithOp::Mul => l * r,
            ArithOp::Div => {
                if r == 0.0 {
                    return Err(GqlError::type_error("division by zero"));
                }
                l / r
            }
            ArithOp::Mod => {
                if r == 0.0 {
                    return Err(GqlError::type_error("modulo by zero"));
                }
                l % r
            }
        };
        return Ok(GqlValue::Float(result));
    }

    // Integer arithmetic
    match (left, right) {
        (GqlValue::Int(l), GqlValue::Int(r)) => {
            let result = match op {
                ArithOp::Add => l.checked_add(*r),
                ArithOp::Sub => l.checked_sub(*r),
                ArithOp::Mul => l.checked_mul(*r),
                ArithOp::Div => {
                    if *r == 0 {
                        return Err(GqlError::type_error("division by zero"));
                    }
                    l.checked_div(*r)
                }
                ArithOp::Mod => {
                    if *r == 0 {
                        return Err(GqlError::type_error("modulo by zero"));
                    }
                    l.checked_rem(*r)
                }
            };
            Ok(GqlValue::Int(
                result.ok_or_else(|| GqlError::type_error("integer overflow"))?,
            ))
        }
        (GqlValue::UInt(l), GqlValue::UInt(r)) => {
            let result = match op {
                ArithOp::Add => l.checked_add(*r),
                ArithOp::Sub => l.checked_sub(*r),
                ArithOp::Mul => l.checked_mul(*r),
                ArithOp::Div => {
                    if *r == 0 {
                        return Err(GqlError::type_error("division by zero"));
                    }
                    l.checked_div(*r)
                }
                ArithOp::Mod => {
                    if *r == 0 {
                        return Err(GqlError::type_error("modulo by zero"));
                    }
                    l.checked_rem(*r)
                }
            };
            Ok(GqlValue::UInt(result.ok_or_else(|| {
                GqlError::type_error("unsigned integer overflow")
            })?))
        }
        // Mixed Int + UInt → coerce to signed (rule 2)
        (GqlValue::Int(l), GqlValue::UInt(r)) => {
            let r_signed = i64::try_from(*r)
                .map_err(|_| GqlError::type_error("UINT64 value exceeds INT64 range"))?;
            eval_arithmetic(&GqlValue::Int(*l), op, &GqlValue::Int(r_signed), ctx)
        }
        (GqlValue::UInt(l), GqlValue::Int(r)) => {
            let l_signed = i64::try_from(*l)
                .map_err(|_| GqlError::type_error("UINT64 value exceeds INT64 range"))?;
            eval_arithmetic(&GqlValue::Int(l_signed), op, &GqlValue::Int(*r), ctx)
        }
        // Implicit coercion fallback: try to coerce strings to numbers
        _ => {
            if ctx.options.strict_coercion {
                return Err(crate::types::coercion::strict_type_error(
                    &left.gql_type().to_string(),
                    &right.gql_type().to_string(),
                    &format!("apply {} to", arith_op_name(op)),
                ));
            }
            use crate::types::coercion::try_coerce_to_float;
            if let (Some(l), Some(r)) = (try_coerce_to_float(left), try_coerce_to_float(right)) {
                let result = match op {
                    ArithOp::Add => l + r,
                    ArithOp::Sub => l - r,
                    ArithOp::Mul => l * r,
                    ArithOp::Div => {
                        if r == 0.0 {
                            return Err(GqlError::type_error("division by zero"));
                        }
                        l / r
                    }
                    ArithOp::Mod => {
                        if r == 0.0 {
                            return Err(GqlError::type_error("modulo by zero"));
                        }
                        l % r
                    }
                };
                return Ok(GqlValue::Float(result));
            }
            Err(GqlError::type_error(format!(
                "cannot apply {} to {} and {}",
                arith_op_name(op),
                left.gql_type(),
                right.gql_type()
            )))
        }
    }
}

pub(crate) fn arith_op_name(op: ArithOp) -> &'static str {
    match op {
        ArithOp::Add => "+",
        ArithOp::Sub => "-",
        ArithOp::Mul => "*",
        ArithOp::Div => "/",
        ArithOp::Mod => "%",
    }
}

pub(crate) fn eval_negate(val: &GqlValue) -> Result<GqlValue, GqlError> {
    match val {
        GqlValue::Null => Ok(GqlValue::Null),
        GqlValue::Int(i) => match i.checked_neg() {
            Some(n) => Ok(GqlValue::Int(n)),
            None => Err(GqlError::type_error("integer overflow on negation")),
        },
        GqlValue::Float(f) => Ok(GqlValue::Float(-f)),
        _ => Err(GqlError::type_error(format!(
            "cannot negate {}",
            val.gql_type()
        ))),
    }
}

// ── CAST ───────────────────────────────────────────────────────────

pub(crate) fn eval_cast(val: &GqlValue, target: &GqlType) -> Result<GqlValue, GqlError> {
    if val.is_null() {
        return Ok(GqlValue::Null);
    }

    match target {
        GqlType::String => Ok(GqlValue::String(SmolStr::new(
            super::eval::value_to_string(val).as_str(),
        ))),
        GqlType::Int => match val {
            GqlValue::Int(_) => Ok(val.clone()),
            GqlValue::UInt(u) => {
                Ok(GqlValue::Int(i64::try_from(*u).map_err(|_| {
                    GqlError::type_error("UINT64 exceeds INT64 range")
                })?))
            }
            GqlValue::Float(f) => {
                if !f.is_finite() {
                    return Err(GqlError::type_error("cannot cast NaN/Infinity to INT64"));
                }
                if *f > i64::MAX as f64 || *f < i64::MIN as f64 {
                    return Err(GqlError::type_error("float value out of INT64 range"));
                }
                Ok(GqlValue::Int(*f as i64))
            }
            GqlValue::String(s) => {
                let i: i64 = s
                    .parse()
                    .map_err(|_| GqlError::type_error(format!("cannot cast '{s}' to INT64")))?;
                Ok(GqlValue::Int(i))
            }
            _ => Err(GqlError::type_error(format!(
                "cannot cast {} to INT64",
                val.gql_type()
            ))),
        },
        GqlType::Float => match val {
            GqlValue::Float(_) => Ok(val.clone()),
            GqlValue::Int(i) => Ok(GqlValue::Float(*i as f64)),
            GqlValue::UInt(u) => Ok(GqlValue::Float(*u as f64)),
            GqlValue::String(s) => {
                let f: f64 = s
                    .parse()
                    .map_err(|_| GqlError::type_error(format!("cannot cast '{s}' to DOUBLE")))?;
                Ok(GqlValue::Float(f))
            }
            _ => Err(GqlError::type_error(format!(
                "cannot cast {} to DOUBLE",
                val.gql_type()
            ))),
        },
        GqlType::Bool => match val {
            GqlValue::Bool(_) => Ok(val.clone()),
            GqlValue::String(s) => match s.to_lowercase().as_str() {
                "true" => Ok(GqlValue::Bool(true)),
                "false" => Ok(GqlValue::Bool(false)),
                _ => Err(GqlError::type_error(format!("cannot cast '{s}' to BOOL"))),
            },
            _ => Err(GqlError::type_error(format!(
                "cannot cast {} to BOOL",
                val.gql_type()
            ))),
        },
        GqlType::UInt => match val {
            GqlValue::UInt(_) => Ok(val.clone()),
            GqlValue::Int(i) => {
                Ok(GqlValue::UInt(u64::try_from(*i).map_err(|_| {
                    GqlError::type_error("negative value cannot cast to UINT64")
                })?))
            }
            GqlValue::Float(f) => {
                if !f.is_finite() || *f < 0.0 || *f > u64::MAX as f64 {
                    return Err(GqlError::type_error(
                        "cannot cast float to UINT64: out of range or NaN",
                    ));
                }
                Ok(GqlValue::UInt(*f as u64))
            }
            GqlValue::String(s) => {
                let u: u64 = s
                    .parse()
                    .map_err(|_| GqlError::type_error(format!("cannot cast '{s}' to UINT64")))?;
                Ok(GqlValue::UInt(u))
            }
            _ => Err(GqlError::type_error(format!(
                "cannot cast {} to UINT64",
                val.gql_type()
            ))),
        },
        GqlType::Date => match val {
            GqlValue::Date(_) => Ok(val.clone()),
            GqlValue::String(s) => {
                // Parse ISO date: YYYY-MM-DD (handle optional leading - for negative years)
                let (neg_year, date_str) = if let Some(rest) = s.strip_prefix('-') {
                    (true, rest)
                } else {
                    (false, s.as_str())
                };
                let parts: Vec<&str> = date_str.split('-').collect();
                if parts.len() != 3 {
                    return Err(GqlError::type_error(format!(
                        "cannot cast '{s}' to DATE, expected YYYY-MM-DD"
                    )));
                }
                let y: i32 = parts[0]
                    .parse()
                    .map_err(|_| GqlError::type_error(format!("invalid year in '{s}'")))?;
                let y = if neg_year { -y } else { y };
                let m: u32 = parts[1]
                    .parse()
                    .map_err(|_| GqlError::type_error(format!("invalid month in '{s}'")))?;
                let d: u32 = parts[2]
                    .parse()
                    .map_err(|_| GqlError::type_error(format!("invalid day in '{s}'")))?;
                if !(1..=12).contains(&m) {
                    return Err(GqlError::type_error(format!("invalid month {m} in '{s}'")));
                }
                if !(1..=31).contains(&d) {
                    return Err(GqlError::type_error(format!("invalid day {d} in '{s}'")));
                }
                Ok(GqlValue::Date(crate::types::value::GqlDate {
                    days: ymd_to_epoch_days(y, m, d),
                }))
            }
            GqlValue::ZonedDateTime(zdt) => {
                let epoch_days = (zdt.nanos.div_euclid(1_000_000_000 * 86400)) as i32;
                Ok(GqlValue::Date(crate::types::value::GqlDate {
                    days: epoch_days,
                }))
            }
            GqlValue::LocalDateTime(dt) => {
                let epoch_days = (dt.nanos.div_euclid(1_000_000_000 * 86400)) as i32;
                Ok(GqlValue::Date(crate::types::value::GqlDate {
                    days: epoch_days,
                }))
            }
            _ => Err(GqlError::type_error(format!(
                "cannot cast {} to DATE",
                val.gql_type()
            ))),
        },
        GqlType::ZonedDateTime => match val {
            GqlValue::ZonedDateTime(_) => Ok(val.clone()),
            GqlValue::String(_s) => {
                // String→ZonedDateTime parsing is complex (ISO 8601 with timezone)
                // Defer to the zoned_datetime() function at runtime
                Err(GqlError::type_error(
                    "CAST string to ZONED DATETIME: use ZONED DATETIME '<value>' syntax",
                ))
            }
            GqlValue::Date(d) => Ok(GqlValue::ZonedDateTime(
                crate::types::value::ZonedDateTime {
                    nanos: i64::from(d.days) * 86400 * 1_000_000_000,
                    offset_seconds: 0,
                },
            )),
            GqlValue::LocalDateTime(dt) => Ok(GqlValue::ZonedDateTime(
                crate::types::value::ZonedDateTime {
                    nanos: dt.nanos,
                    offset_seconds: 0,
                },
            )),
            _ => Err(GqlError::type_error(format!(
                "cannot cast {} to ZONED DATETIME",
                val.gql_type()
            ))),
        },
        GqlType::LocalDateTime => match val {
            GqlValue::LocalDateTime(_) => Ok(val.clone()),
            GqlValue::ZonedDateTime(zdt) => Ok(GqlValue::LocalDateTime(
                crate::types::value::GqlLocalDateTime { nanos: zdt.nanos },
            )),
            GqlValue::Date(d) => Ok(GqlValue::LocalDateTime(
                crate::types::value::GqlLocalDateTime {
                    nanos: i64::from(d.days) * 86400 * 1_000_000_000,
                },
            )),
            _ => Err(GqlError::type_error(format!(
                "cannot cast {} to LOCAL DATETIME",
                val.gql_type()
            ))),
        },
        GqlType::Duration => match val {
            GqlValue::Duration(_) => Ok(val.clone()),
            GqlValue::String(s) => {
                // Simple duration parsing: PT1H30M, PT45S, etc.
                let d = parse_duration(s).map_err(|e| {
                    GqlError::type_error(format!("cannot cast '{s}' to DURATION: {e}"))
                })?;
                Ok(GqlValue::Duration(crate::types::value::GqlDuration {
                    nanos: d,
                }))
            }
            _ => Err(GqlError::type_error(format!(
                "cannot cast {} to DURATION",
                val.gql_type()
            ))),
        },
        _ => Err(GqlError::type_error(format!(
            "unsupported CAST target type: {target}"
        ))),
    }
}

/// Convert (year, month, day) to epoch days (inverse of epoch_days_to_ymd).
pub fn ymd_to_epoch_days(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 {
        i64::from(y) - 1
    } else {
        i64::from(y)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + i64::from(doe) - 719_468) as i32
}

// ── Duration parsing ───────────────────────────────────────────────

/// Parse a duration string to nanoseconds.
/// Accepts both Selene format ("1h30m", "7d") and ISO 8601 ("PT1H30M", "P1DT2H", "PT45S").
/// Case-insensitive for unit suffixes.
///
/// Known limitation: 'M' is always interpreted as minutes, not months.
/// ISO 8601 `P1M` (1 month) will be parsed as 1 minute. Month-duration
/// support is not implemented because months have variable-length days.
pub fn parse_duration(s: &str) -> Result<i64, GqlError> {
    let err = || GqlError::InvalidArgument {
        message: format!("invalid duration: '{s}'"),
    };

    // Handle optional negative sign
    let (negative, s) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else {
        (false, s)
    };

    // Strip ISO 8601 'P' prefix
    let s = s
        .strip_prefix('P')
        .or_else(|| s.strip_prefix('p'))
        .unwrap_or(s);
    // Strip 'T' time designator (may appear after P or after date portion)
    // For simplicity, just strip all T characters that aren't part of numbers
    let s = s
        .strip_prefix('T')
        .or_else(|| s.strip_prefix('t'))
        .unwrap_or(s);

    let mut total_nanos: i64 = 0;
    let mut current_num = String::new();
    let mut in_time_part = false; // after 'T' in ISO format like P1DT2H

    for ch in s.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            current_num.push(ch);
        } else if ch == 'T' || ch == 't' {
            // T separates date from time portion (e.g., P1DT2H)
            if !current_num.is_empty() {
                return Err(err()); // number without unit before T
            }
            in_time_part = true;
        } else {
            if current_num.is_empty() {
                return Err(err());
            }
            let n: f64 = current_num.parse().map_err(|_| err())?;
            current_num.clear();
            let nanos_per_unit: f64 = match ch.to_ascii_lowercase() {
                's' => 1_000_000_000.0,
                'm' if in_time_part || !s.contains('T') && !s.contains('t') => 60_000_000_000.0,
                'h' => 3_600_000_000_000.0,
                'd' => 86_400_000_000_000.0,
                'y' => 365.25 * 86_400_000_000_000.0, // approximate year
                _ => return Err(err()),
            };
            total_nanos = total_nanos.saturating_add((n * nanos_per_unit) as i64);
        }
    }

    if !current_num.is_empty() {
        // Bare number without unit: treat as seconds for ISO compat
        let n: f64 = current_num.parse().map_err(|_| err())?;
        total_nanos = total_nanos.saturating_add((n * 1_000_000_000.0) as i64);
    }

    if total_nanos == 0 && s.is_empty() {
        return Err(err());
    }

    Ok(if negative { -total_nanos } else { total_nanos })
}
