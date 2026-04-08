//! Temporal, vector similarity, and text matching scalar functions.

use smol_str::SmolStr;

use super::functions_core::parse_tz_offset;
use super::{EvalContext, GqlError, GqlValue, ScalarFunction};

// ── Temporal functions ───────────────────────────────────────────

pub(crate) struct NowFunction;
impl ScalarFunction for NowFunction {
    fn name(&self) -> &'static str {
        "now"
    }
    fn invoke(&self, _args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        Ok(GqlValue::ZonedDateTime(
            crate::types::value::ZonedDateTime::from_nanos_utc(selene_core::now_nanos()),
        ))
    }
}

pub(crate) struct CurrentDateFunction;
impl ScalarFunction for CurrentDateFunction {
    fn name(&self) -> &'static str {
        "current_date"
    }
    fn invoke(&self, _args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let nanos = selene_core::now_nanos();
        let days = (nanos / 1_000_000_000).div_euclid(86400) as i32;
        Ok(GqlValue::Date(crate::types::value::GqlDate { days }))
    }
}

pub(crate) struct CurrentTimeFunction;
impl ScalarFunction for CurrentTimeFunction {
    fn name(&self) -> &'static str {
        "current_time"
    }
    fn invoke(&self, _args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let nanos = selene_core::now_nanos();
        let day_nanos = (nanos % (86400 * 1_000_000_000)) as u64;
        Ok(GqlValue::ZonedTime(crate::types::value::GqlZonedTime {
            nanos: day_nanos,
            offset_seconds: 0,
        }))
    }
}

pub(crate) struct ExtractFunction;
impl ScalarFunction for ExtractFunction {
    fn name(&self) -> &'static str {
        "extract"
    }
    fn description(&self) -> &'static str {
        "Extract date/time component"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let field = match args.first() {
            Some(GqlValue::String(s)) => s.to_uppercase(),
            _ => {
                return Err(GqlError::type_error(
                    "extract(field, temporal): field must be a string",
                ));
            }
        };
        let temporal = args
            .get(1)
            .ok_or_else(|| GqlError::type_error("extract requires 2 arguments"))?;
        if temporal.is_null() {
            return Ok(GqlValue::Null);
        }

        // Extract from Duration -- signed components (negative durations yield negative parts)
        if let GqlValue::Duration(d) = temporal {
            let total_secs = d.nanos / 1_000_000_000; // preserves sign
            return match field.as_str() {
                "DAY" | "DAYS" => Ok(GqlValue::Int(total_secs / 86400)),
                "HOUR" | "HOURS" => Ok(GqlValue::Int((total_secs % 86400) / 3600)),
                "MINUTE" | "MINUTES" => Ok(GqlValue::Int((total_secs % 3600) / 60)),
                "SECOND" | "SECONDS" => Ok(GqlValue::Int(total_secs % 60)),
                _ => Err(GqlError::InvalidArgument {
                    message: format!("cannot extract {field} from DURATION"),
                }),
            };
        }

        // Extract from time-of-day types
        let time_nanos = match temporal {
            GqlValue::LocalTime(t) => Some(t.nanos),
            GqlValue::ZonedTime(t) => Some(t.nanos),
            _ => None,
        };
        if let Some(tn) = time_nanos {
            let total_secs = (tn / 1_000_000_000) as i64;
            return match field.as_str() {
                "HOUR" => Ok(GqlValue::Int(total_secs / 3600)),
                "MINUTE" => Ok(GqlValue::Int((total_secs % 3600) / 60)),
                "SECOND" => Ok(GqlValue::Int(total_secs % 60)),
                _ => Err(GqlError::InvalidArgument {
                    message: format!("cannot extract {field} from TIME"),
                }),
            };
        }

        // Extract from date/datetime types (get epoch nanos)
        let nanos = match temporal {
            GqlValue::Int(n) => *n,
            GqlValue::ZonedDateTime(zdt) => zdt.nanos,
            GqlValue::LocalDateTime(dt) => dt.nanos,
            GqlValue::Date(d) => i64::from(d.days) * 86400 * 1_000_000_000,
            _ => {
                return Err(GqlError::type_error(format!(
                    "extract: unsupported type {}",
                    temporal.gql_type()
                )));
            }
        };
        let secs = nanos / 1_000_000_000;
        let days = secs.div_euclid(86400);
        let (y, m, d) = civil_from_days_inverse(days);
        let day_secs = secs.rem_euclid(86400);
        match field.as_str() {
            "YEAR" => Ok(GqlValue::Int(i64::from(y))),
            "MONTH" => Ok(GqlValue::Int(i64::from(m))),
            "DAY" => Ok(GqlValue::Int(i64::from(d))),
            "HOUR" => Ok(GqlValue::Int(day_secs / 3600)),
            "MINUTE" => Ok(GqlValue::Int((day_secs % 3600) / 60)),
            "SECOND" => Ok(GqlValue::Int(day_secs % 60)),
            "EPOCH" => Ok(GqlValue::Int(secs)),
            _ => Err(GqlError::InvalidArgument {
                message: format!("unknown extract field: {field}"),
            }),
        }
    }
}

pub(crate) struct DateAddFunction;
impl ScalarFunction for DateAddFunction {
    fn name(&self) -> &'static str {
        "date_add"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let ts = match args.first() {
            Some(GqlValue::Int(n)) => *n,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("date_add(timestamp, nanos)")),
        };
        let delta = match args.get(1) {
            Some(GqlValue::Int(n)) => *n,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("date_add(timestamp, nanos)")),
        };
        Ok(GqlValue::Int(ts.saturating_add(delta)))
    }
}

pub(crate) struct DateSubFunction;
impl ScalarFunction for DateSubFunction {
    fn name(&self) -> &'static str {
        "date_sub"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let ts = match args.first() {
            Some(GqlValue::Int(n)) => *n,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("date_sub(timestamp, nanos)")),
        };
        let delta = match args.get(1) {
            Some(GqlValue::Int(n)) => *n,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("date_sub(timestamp, nanos)")),
        };
        Ok(GqlValue::Int(ts.saturating_sub(delta)))
    }
}

pub(crate) struct TimestampToStringFunction;
impl ScalarFunction for TimestampToStringFunction {
    fn name(&self) -> &'static str {
        "timestamp_to_string"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let nanos = match args.first() {
            Some(GqlValue::Int(n)) => *n,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("timestamp_to_string(nanos)")),
        };
        let secs = nanos / 1_000_000_000;
        let days = secs.div_euclid(86400);
        let (y, m, d) = civil_from_days_inverse(days);
        let day_secs = secs.rem_euclid(86400);
        let h = day_secs / 3600;
        let mi = (day_secs % 3600) / 60;
        let s = day_secs % 60;
        Ok(GqlValue::String(SmolStr::new(format!(
            "{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z"
        ))))
    }
}

/// Convert days since epoch to (year, month, day). Inverse of days_from_civil.
fn civil_from_days_inverse(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ── Temporal constructors ──────────────────────────────────────

pub(crate) struct LocalTimeFunction;
impl ScalarFunction for LocalTimeFunction {
    fn name(&self) -> &'static str {
        "local_time"
    }
    fn description(&self) -> &'static str {
        "Current local time"
    }
    fn invoke(&self, _args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let nanos = selene_core::now_nanos();
        let day_nanos = (nanos % (86400 * 1_000_000_000)) as u64;
        Ok(GqlValue::LocalTime(crate::types::value::GqlLocalTime {
            nanos: day_nanos,
        }))
    }
}

pub(crate) struct LocalDatetimeFunction;
impl ScalarFunction for LocalDatetimeFunction {
    fn name(&self) -> &'static str {
        "local_datetime"
    }
    fn description(&self) -> &'static str {
        "Current local datetime"
    }
    fn invoke(&self, _args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let nanos = selene_core::now_nanos();
        Ok(GqlValue::LocalDateTime(
            crate::types::value::GqlLocalDateTime { nanos },
        ))
    }
}

pub(crate) struct DateConstructorFunction;
impl ScalarFunction for DateConstructorFunction {
    fn name(&self) -> &'static str {
        "date"
    }
    fn description(&self) -> &'static str {
        "Parse ISO date string to DATE"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::String(s)) => {
                let parts: Vec<&str> = s.split('-').collect();
                if parts.len() != 3 {
                    return Err(GqlError::type_error(format!("invalid date: {s}")));
                }
                let y: i32 = parts[0]
                    .parse()
                    .map_err(|_| GqlError::type_error(format!("invalid year: {s}")))?;
                let m: u32 = parts[1]
                    .parse()
                    .map_err(|_| GqlError::type_error(format!("invalid month: {s}")))?;
                let d: u32 = parts[2]
                    .parse()
                    .map_err(|_| GqlError::type_error(format!("invalid day: {s}")))?;
                Ok(GqlValue::Date(crate::types::value::GqlDate {
                    days: crate::runtime::eval::ymd_to_epoch_days(y, m, d),
                }))
            }
            Some(GqlValue::Null) => Ok(GqlValue::Null),
            // No arguments: return current date (same as current_date()).
            None => {
                let nanos = selene_core::now_nanos();
                let days = (nanos / 1_000_000_000).div_euclid(86400) as i32;
                Ok(GqlValue::Date(crate::types::value::GqlDate { days }))
            }
            _ => Err(GqlError::type_error(
                "date() requires a string argument or no arguments",
            )),
        }
    }
}

pub(crate) struct TimeConstructorFunction;
impl ScalarFunction for TimeConstructorFunction {
    fn name(&self) -> &'static str {
        "time"
    }
    fn description(&self) -> &'static str {
        "Parse ISO time string"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::String(s)) => {
                let parts: Vec<&str> = s.split(':').collect();
                if parts.len() < 2 {
                    return Err(GqlError::type_error(format!("invalid time: {s}")));
                }
                let h: u64 = parts[0].parse().unwrap_or(0);
                let m: u64 = parts[1].parse().unwrap_or(0);
                let sec: u64 = parts
                    .get(2)
                    .and_then(|v| v.split('.').next()?.parse().ok())
                    .unwrap_or(0);
                let nanos = (h * 3600 + m * 60 + sec) * 1_000_000_000;
                Ok(GqlValue::LocalTime(crate::types::value::GqlLocalTime {
                    nanos,
                }))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("time() requires a string argument")),
        }
    }
}

pub(crate) struct ZonedTimeConstructorFunction;
impl ScalarFunction for ZonedTimeConstructorFunction {
    fn name(&self) -> &'static str {
        "zoned_time"
    }
    fn description(&self) -> &'static str {
        "Parse time with timezone offset"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::String(s)) => {
                let (time_part, offset_secs) = if s.ends_with('Z') || s.ends_with('z') {
                    (&s[..s.len() - 1], 0i32)
                } else if let Some(pos) = s.rfind('+') {
                    let off = parse_tz_offset(&s[pos..])?;
                    (&s[..pos], off)
                } else if let Some(pos) = s[1..].rfind('-') {
                    // Skip first char (could be part of time), find last '-'
                    let actual_pos = pos + 1;
                    let off = parse_tz_offset(&s[actual_pos..])?;
                    (&s[..actual_pos], off)
                } else {
                    (s.as_str(), 0)
                };
                let parts: Vec<&str> = time_part.split(':').collect();
                let h: u64 = parts.first().and_then(|v| v.parse().ok()).unwrap_or(0);
                let m: u64 = parts.get(1).and_then(|v| v.parse().ok()).unwrap_or(0);
                let sec: u64 = parts
                    .get(2)
                    .and_then(|v| v.split('.').next()?.parse().ok())
                    .unwrap_or(0);
                let nanos = (h * 3600 + m * 60 + sec) * 1_000_000_000;
                Ok(GqlValue::ZonedTime(crate::types::value::GqlZonedTime {
                    nanos,
                    offset_seconds: offset_secs,
                }))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("zoned_time() requires a string")),
        }
    }
}

// ── Duration between ─────────────────────────────────────────────

pub(crate) struct DurationBetweenFunction;
impl ScalarFunction for DurationBetweenFunction {
    fn name(&self) -> &'static str {
        "duration_between"
    }
    fn description(&self) -> &'static str {
        "Duration between two temporal instants"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a_nanos = match args.first() {
            Some(GqlValue::ZonedDateTime(zdt)) => zdt.nanos,
            Some(GqlValue::LocalDateTime(dt)) => dt.nanos,
            Some(GqlValue::Date(d)) => i64::from(d.days) * 86400 * 1_000_000_000,
            Some(GqlValue::Null) | None => return Ok(GqlValue::Null),
            _ => {
                return Err(GqlError::type_error(
                    "duration_between: first arg must be temporal",
                ));
            }
        };
        let b_nanos = match args.get(1) {
            Some(GqlValue::ZonedDateTime(zdt)) => zdt.nanos,
            Some(GqlValue::LocalDateTime(dt)) => dt.nanos,
            Some(GqlValue::Date(d)) => i64::from(d.days) * 86400 * 1_000_000_000,
            Some(GqlValue::Null) | None => return Ok(GqlValue::Null),
            _ => {
                return Err(GqlError::type_error(
                    "duration_between: second arg must be temporal",
                ));
            }
        };
        let diff = a_nanos
            .checked_sub(b_nanos)
            .ok_or_else(|| GqlError::type_error("duration_between: overflow"))?;
        Ok(GqlValue::Duration(
            crate::types::value::GqlDuration::day_time(diff),
        ))
    }
}

// ── Vector similarity functions ──────────────────────────────────────

/// Extract f32 slice from a GqlValue (Vector or List of floats).
fn extract_f32_slice(val: &GqlValue, fn_name: &str, arg_pos: &str) -> Result<Vec<f32>, GqlError> {
    match val {
        GqlValue::Vector(v) => Ok(v.to_vec()),
        GqlValue::List(l) => {
            let mut floats = Vec::with_capacity(l.elements.len());
            for elem in l.elements.iter() {
                match elem {
                    GqlValue::Float(f) => floats.push(*f as f32),
                    GqlValue::Int(i) => floats.push(*i as f32),
                    _ => {
                        return Err(GqlError::type_error(format!(
                            "{fn_name}: {arg_pos} list element must be numeric, got {}",
                            elem.gql_type()
                        )));
                    }
                }
            }
            Ok(floats)
        }
        GqlValue::Null => Err(GqlError::type_error(format!(
            "{fn_name}: {arg_pos} is NULL"
        ))),
        _ => Err(GqlError::type_error(format!(
            "{fn_name}: {arg_pos} must be VECTOR or LIST<DOUBLE>, got {}",
            val.gql_type()
        ))),
    }
}

pub(crate) struct CosineSimilarityFunction;
impl ScalarFunction for CosineSimilarityFunction {
    fn name(&self) -> &'static str {
        "cosine_similarity"
    }
    fn description(&self) -> &'static str {
        "Cosine similarity between two vectors (-1.0 to 1.0)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        if args.len() != 2 {
            return Err(GqlError::InvalidArgument {
                message: "cosine_similarity requires 2 arguments".into(),
            });
        }
        if args[0].is_null() || args[1].is_null() {
            return Ok(GqlValue::Null);
        }
        let a = extract_f32_slice(&args[0], "cosine_similarity", "first argument")?;
        let b = extract_f32_slice(&args[1], "cosine_similarity", "second argument")?;
        if a.len() != b.len() {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "cosine_similarity: dimension mismatch ({} vs {})",
                    a.len(),
                    b.len()
                ),
            });
        }
        let dot: f32 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
        let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        let sim = if mag_a == 0.0 || mag_b == 0.0 {
            0.0
        } else {
            dot / (mag_a * mag_b)
        };
        Ok(GqlValue::Float(f64::from(sim)))
    }
}

pub(crate) struct EuclideanDistanceFunction;
impl ScalarFunction for EuclideanDistanceFunction {
    fn name(&self) -> &'static str {
        "euclidean_distance"
    }
    fn description(&self) -> &'static str {
        "Euclidean distance between two vectors"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        if args.len() != 2 {
            return Err(GqlError::InvalidArgument {
                message: "euclidean_distance requires 2 arguments".into(),
            });
        }
        if args[0].is_null() || args[1].is_null() {
            return Ok(GqlValue::Null);
        }
        let a = extract_f32_slice(&args[0], "euclidean_distance", "first argument")?;
        let b = extract_f32_slice(&args[1], "euclidean_distance", "second argument")?;
        if a.len() != b.len() {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "euclidean_distance: dimension mismatch ({} vs {})",
                    a.len(),
                    b.len()
                ),
            });
        }
        let dist: f32 = a
            .iter()
            .zip(&b)
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt();
        Ok(GqlValue::Float(f64::from(dist)))
    }
}

// ── Text matching ───────────────────────────────────────────────────

pub(crate) struct TextMatchFunction;
impl ScalarFunction for TextMatchFunction {
    fn name(&self) -> &'static str {
        "text_match"
    }
    fn description(&self) -> &'static str {
        "Check if all query words appear in the text (case-insensitive)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        if args.len() != 2 {
            return Err(GqlError::InvalidArgument {
                message: "text_match requires 2 arguments: text, query".into(),
            });
        }
        if args[0].is_null() || args[1].is_null() {
            return Ok(GqlValue::Null);
        }
        let text = args[0].as_str()?;
        let query = args[1].as_str()?;
        let text_lower = text.to_lowercase();
        let matched = query
            .split_whitespace()
            .all(|word| text_lower.contains(&word.to_lowercase()));
        Ok(GqlValue::Bool(matched))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::functions::FunctionRegistry;
    use crate::types::value::{GqlDate, GqlDuration, GqlLocalDateTime, ZonedDateTime};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;

    fn ctx() -> (SeleneGraph, FunctionRegistry) {
        (SeleneGraph::new(), FunctionRegistry::with_builtins())
    }

    fn s(val: &str) -> GqlValue {
        GqlValue::String(SmolStr::new(val))
    }

    // ── DateConstructorFunction ──

    #[test]
    fn date_constructor_valid() {
        let f = DateConstructorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[s("2024-01-15")], &c).unwrap();
        if let GqlValue::Date(d) = r {
            // 2024-01-15 should be a specific number of days since epoch
            assert!(d.days > 0);
        } else {
            panic!("expected Date");
        }
    }

    #[test]
    fn date_constructor_invalid_format() {
        let f = DateConstructorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert!(f.invoke(&[s("not-a-date")], &c).is_err());
    }

    #[test]
    fn date_constructor_null_returns_null() {
        let f = DateConstructorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    // ── TimeConstructorFunction ──

    #[test]
    fn time_constructor_valid() {
        let f = TimeConstructorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[s("14:30:00")], &c).unwrap();
        if let GqlValue::LocalTime(t) = r {
            // 14h30m = 52200 seconds = 52200_000_000_000 nanos
            assert_eq!(t.nanos, 52_200_000_000_000);
        } else {
            panic!("expected LocalTime");
        }
    }

    #[test]
    fn time_constructor_hours_minutes_only() {
        let f = TimeConstructorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[s("08:15")], &c).unwrap();
        if let GqlValue::LocalTime(t) = r {
            assert_eq!(t.nanos, (8 * 3600 + 15 * 60) * 1_000_000_000);
        } else {
            panic!("expected LocalTime");
        }
    }

    #[test]
    fn time_constructor_null_returns_null() {
        let f = TimeConstructorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    // ── ZonedTimeConstructorFunction ──

    #[test]
    fn zoned_time_with_z() {
        let f = ZonedTimeConstructorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[s("10:30:00Z")], &c).unwrap();
        if let GqlValue::ZonedTime(t) = r {
            assert_eq!(t.offset_seconds, 0);
            assert_eq!(t.nanos, (10 * 3600 + 30 * 60) * 1_000_000_000);
        } else {
            panic!("expected ZonedTime");
        }
    }

    #[test]
    fn zoned_time_with_positive_offset() {
        let f = ZonedTimeConstructorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[s("10:30:00+02:00")], &c).unwrap();
        if let GqlValue::ZonedTime(t) = r {
            assert_eq!(t.offset_seconds, 7200);
        } else {
            panic!("expected ZonedTime");
        }
    }

    #[test]
    fn zoned_time_with_negative_offset() {
        let f = ZonedTimeConstructorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[s("10:30:00-05:00")], &c).unwrap();
        if let GqlValue::ZonedTime(t) = r {
            assert_eq!(t.offset_seconds, -18000); // -5h in seconds
            assert_eq!(t.nanos, (10 * 3600 + 30 * 60) * 1_000_000_000);
        } else {
            panic!("expected ZonedTime");
        }
    }

    // ── ExtractFunction ──

    #[test]
    fn extract_year_from_zoned_datetime() {
        let f = ExtractFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // 2024-01-15T00:00:00Z
        let zdt = GqlValue::ZonedDateTime(ZonedDateTime {
            nanos: 19737 * 86400 * 1_000_000_000_i64,
            offset_seconds: 0,
        });
        let r = f.invoke(&[s("YEAR"), zdt], &c).unwrap();
        assert_eq!(r, GqlValue::Int(2024));
    }

    #[test]
    fn extract_month_from_date() {
        let f = ExtractFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // 2024-06-15
        let d = GqlValue::Date(GqlDate {
            days: crate::runtime::eval::ymd_to_epoch_days(2024, 6, 15),
        });
        let r = f.invoke(&[s("MONTH"), d], &c).unwrap();
        assert_eq!(r, GqlValue::Int(6));
    }

    #[test]
    fn extract_day_from_date() {
        let f = ExtractFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let d = GqlValue::Date(GqlDate {
            days: crate::runtime::eval::ymd_to_epoch_days(2024, 3, 25),
        });
        let r = f.invoke(&[s("DAY"), d], &c).unwrap();
        assert_eq!(r, GqlValue::Int(25));
    }

    #[test]
    fn extract_hour_from_local_datetime() {
        let f = ExtractFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // Construct a local datetime at 2024-01-15 14:30:00
        let nanos =
            19737_i64 * 86400 * 1_000_000_000 + 14 * 3_600_000_000_000 + 30 * 60_000_000_000;
        let ldt = GqlValue::LocalDateTime(GqlLocalDateTime { nanos });
        let r = f.invoke(&[s("HOUR"), ldt], &c).unwrap();
        assert_eq!(r, GqlValue::Int(14));
    }

    #[test]
    fn extract_from_duration() {
        let f = ExtractFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // 1 day 2 hours 30 minutes = 95400 seconds
        let dur = GqlValue::Duration(GqlDuration::day_time(
            (86400 + 2 * 3600 + 30 * 60) * 1_000_000_000,
        ));
        assert_eq!(
            f.invoke(&[s("DAY"), dur.clone()], &c).unwrap(),
            GqlValue::Int(1)
        );
        assert_eq!(
            f.invoke(&[s("HOUR"), dur.clone()], &c).unwrap(),
            GqlValue::Int(2)
        );
        assert_eq!(
            f.invoke(&[s("MINUTE"), dur], &c).unwrap(),
            GqlValue::Int(30)
        );
    }

    #[test]
    fn extract_null_temporal_returns_null() {
        let f = ExtractFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("YEAR"), GqlValue::Null], &c).unwrap(),
            GqlValue::Null
        );
    }

    #[test]
    fn extract_unknown_field_errors() {
        let f = ExtractFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let d = GqlValue::Date(GqlDate { days: 0 });
        assert!(f.invoke(&[s("QUARTER"), d], &c).is_err());
    }

    // ── DateAddFunction / DateSubFunction ──

    #[test]
    fn date_add_basic() {
        let f = DateAddFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::Int(1000), GqlValue::Int(500)], &c)
            .unwrap();
        assert_eq!(r, GqlValue::Int(1500));
    }

    #[test]
    fn date_sub_basic() {
        let f = DateSubFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::Int(1000), GqlValue::Int(300)], &c)
            .unwrap();
        assert_eq!(r, GqlValue::Int(700));
    }

    #[test]
    fn date_add_null_returns_null() {
        let f = DateAddFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Null, GqlValue::Int(1)], &c).unwrap(),
            GqlValue::Null
        );
    }

    // ── TimestampToStringFunction ──

    #[test]
    fn timestamp_to_string_epoch() {
        let f = TimestampToStringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[GqlValue::Int(0)], &c).unwrap();
        assert_eq!(r, GqlValue::String(SmolStr::new("1970-01-01T00:00:00Z")));
    }

    #[test]
    fn timestamp_to_string_known_date() {
        let f = TimestampToStringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // 2024-01-15T12:30:00Z in nanos
        let nanos =
            19737_i64 * 86400 * 1_000_000_000 + 12 * 3_600_000_000_000 + 30 * 60_000_000_000;
        let r = f.invoke(&[GqlValue::Int(nanos)], &c).unwrap();
        assert_eq!(r, GqlValue::String(SmolStr::new("2024-01-15T12:30:00Z")));
    }

    #[test]
    fn timestamp_to_string_null_returns_null() {
        let f = TimestampToStringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    // ── DurationBetweenFunction ──

    #[test]
    fn duration_between_zoned_datetimes() {
        let f = DurationBetweenFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let a = GqlValue::ZonedDateTime(ZonedDateTime {
            nanos: 2_000_000_000_000,
            offset_seconds: 0,
        });
        let b = GqlValue::ZonedDateTime(ZonedDateTime {
            nanos: 1_000_000_000_000,
            offset_seconds: 0,
        });
        let r = f.invoke(&[a, b], &c).unwrap();
        if let GqlValue::Duration(d) = r {
            assert_eq!(d.nanos, 1_000_000_000_000);
        } else {
            panic!("expected Duration");
        }
    }

    #[test]
    fn duration_between_dates() {
        let f = DurationBetweenFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let a = GqlValue::Date(GqlDate { days: 10 });
        let b = GqlValue::Date(GqlDate { days: 7 });
        let r = f.invoke(&[a, b], &c).unwrap();
        if let GqlValue::Duration(d) = r {
            // 3 days in nanos
            assert_eq!(d.nanos, 3 * 86400 * 1_000_000_000);
        } else {
            panic!("expected Duration");
        }
    }

    #[test]
    fn duration_between_null_returns_null() {
        let f = DurationBetweenFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let a = GqlValue::ZonedDateTime(ZonedDateTime {
            nanos: 1_000,
            offset_seconds: 0,
        });
        assert_eq!(f.invoke(&[a, GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    // ── NowFunction / CurrentDateFunction / CurrentTimeFunction ──

    #[test]
    fn now_returns_zoned_datetime() {
        let f = NowFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[], &c).unwrap();
        assert!(matches!(r, GqlValue::ZonedDateTime(_)));
    }

    #[test]
    fn current_date_returns_date() {
        let f = CurrentDateFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[], &c).unwrap();
        assert!(matches!(r, GqlValue::Date(_)));
    }

    #[test]
    fn current_time_returns_zoned_time() {
        let f = CurrentTimeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[], &c).unwrap();
        assert!(matches!(r, GqlValue::ZonedTime(_)));
    }

    // ── TextMatchFunction ──

    #[test]
    fn text_match_all_words_present() {
        let f = TextMatchFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[s("Hello World Foo Bar"), s("hello foo")], &c)
            .unwrap();
        assert_eq!(r, GqlValue::Bool(true));
    }

    #[test]
    fn text_match_missing_word() {
        let f = TextMatchFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[s("Hello World"), s("hello missing")], &c)
            .unwrap();
        assert_eq!(r, GqlValue::Bool(false));
    }

    #[test]
    fn text_match_empty_query_matches() {
        let f = TextMatchFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // Empty query has no words, so all(empty) = true
        let r = f.invoke(&[s("anything"), s("")], &c).unwrap();
        assert_eq!(r, GqlValue::Bool(true));
    }

    #[test]
    fn text_match_null_returns_null() {
        let f = TextMatchFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Null, s("query")], &c).unwrap(),
            GqlValue::Null
        );
    }

    // ── civil_from_days_inverse ──

    #[test]
    fn civil_from_days_epoch() {
        let (y, m, d) = civil_from_days_inverse(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }

    #[test]
    fn civil_from_days_known_date() {
        // 2024-01-15 = day 19737
        let (y, m, d) = civil_from_days_inverse(19737);
        assert_eq!((y, m, d), (2024, 1, 15));
    }

    #[test]
    fn civil_from_days_negative() {
        // 1969-12-31 = day -1
        let (y, m, d) = civil_from_days_inverse(-1);
        assert_eq!((y, m, d), (1969, 12, 31));
    }
}
