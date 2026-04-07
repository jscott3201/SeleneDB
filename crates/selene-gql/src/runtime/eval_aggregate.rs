//! Accumulator trait and aggregate evaluation.

use std::sync::Arc;

use crate::ast::expr::*;
use crate::runtime::eval::{EvalContext, eval_expr_ctx};
use crate::types::binding::Binding;
use crate::types::error::GqlError;
use crate::types::value::{GqlList, GqlValue};

// ── Accumulator trait (extensible aggregation) ─────────────────────

/// Accumulator for incremental aggregation. Enables custom aggregates
/// and partial aggregation for federation.
pub trait Accumulator: Send {
    fn accumulate(&mut self, value: &GqlValue) -> Result<(), GqlError>;
    fn finish(&self) -> GqlValue;
}

/// Create an accumulator for a given aggregate operation.
pub fn create_accumulator(op: AggregateOp) -> Box<dyn Accumulator> {
    match op {
        AggregateOp::Count => Box::new(CountAccumulator(0)),
        AggregateOp::Sum => Box::new(SumAccumulator {
            int: 0,
            float: 0.0,
            has_float: false,
            has_value: false,
        }),
        AggregateOp::Avg => Box::new(AvgAccumulator { sum: 0.0, count: 0 }),
        AggregateOp::Min => Box::new(MinMaxAccumulator {
            best: None,
            is_max: false,
        }),
        AggregateOp::Max => Box::new(MinMaxAccumulator {
            best: None,
            is_max: true,
        }),
        AggregateOp::CollectList => Box::new(CollectListAccumulator(Vec::new())),
        AggregateOp::StddevSamp => Box::new(StddevAccumulator {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: true,
        }),
        AggregateOp::StddevPop => Box::new(StddevAccumulator {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: false,
        }),
    }
}

struct CountAccumulator(usize);
impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, value: &GqlValue) -> Result<(), GqlError> {
        if !value.is_null() {
            self.0 += 1;
        }
        Ok(())
    }
    fn finish(&self) -> GqlValue {
        GqlValue::Int(self.0 as i64)
    }
}

struct SumAccumulator {
    int: i64,
    float: f64,
    has_float: bool,
    has_value: bool,
}
impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: &GqlValue) -> Result<(), GqlError> {
        if value.is_null() {
            return Ok(());
        }
        self.has_value = true;
        match value {
            GqlValue::Int(i) => {
                self.int = self
                    .int
                    .checked_add(*i)
                    .ok_or_else(|| GqlError::type_error("SUM integer overflow"))?;
            }
            GqlValue::UInt(u) => {
                let i = i64::try_from(*u)
                    .map_err(|_| GqlError::type_error("SUM: UINT64 value exceeds INT64 range"))?;
                self.int = self
                    .int
                    .checked_add(i)
                    .ok_or_else(|| GqlError::type_error("SUM integer overflow"))?;
            }
            GqlValue::Float(f) => {
                self.has_float = true;
                self.float += f;
            }
            _ => return Err(GqlError::type_error("SUM requires numeric values")),
        }
        Ok(())
    }
    fn finish(&self) -> GqlValue {
        if !self.has_value {
            return GqlValue::Null;
        }
        if self.has_float {
            GqlValue::Float(self.float + self.int as f64)
        } else {
            GqlValue::Int(self.int)
        }
    }
}

struct AvgAccumulator {
    sum: f64,
    count: usize,
}
impl Accumulator for AvgAccumulator {
    fn accumulate(&mut self, value: &GqlValue) -> Result<(), GqlError> {
        if value.is_null() {
            return Ok(());
        }
        self.sum += value.as_float()?;
        self.count += 1;
        Ok(())
    }
    fn finish(&self) -> GqlValue {
        if self.count == 0 {
            GqlValue::Null
        } else {
            GqlValue::Float(self.sum / self.count as f64)
        }
    }
}

struct MinMaxAccumulator {
    best: Option<GqlValue>,
    is_max: bool,
}
impl Accumulator for MinMaxAccumulator {
    fn accumulate(&mut self, value: &GqlValue) -> Result<(), GqlError> {
        if value.is_null() {
            return Ok(());
        }
        self.best = Some(match self.best.take() {
            None => value.clone(),
            Some(current) => {
                let ord = value.sort_order(&current);
                if (self.is_max && ord == std::cmp::Ordering::Greater)
                    || (!self.is_max && ord == std::cmp::Ordering::Less)
                {
                    value.clone()
                } else {
                    current
                }
            }
        });
        Ok(())
    }
    fn finish(&self) -> GqlValue {
        self.best.clone().unwrap_or(GqlValue::Null)
    }
}

struct CollectListAccumulator(Vec<GqlValue>);
impl Accumulator for CollectListAccumulator {
    fn accumulate(&mut self, value: &GqlValue) -> Result<(), GqlError> {
        if !value.is_null() {
            self.0.push(value.clone());
        }
        Ok(())
    }
    fn finish(&self) -> GqlValue {
        let element_type = crate::types::value::infer_list_element_type(&self.0);
        GqlValue::List(GqlList {
            element_type,
            elements: Arc::from(self.0.clone()),
        })
    }
}

/// Welford's online algorithm for variance/stddev.
struct StddevAccumulator {
    count: u64,
    mean: f64,
    m2: f64,
    sample: bool,
}
impl Accumulator for StddevAccumulator {
    fn accumulate(&mut self, value: &GqlValue) -> Result<(), GqlError> {
        if value.is_null() {
            return Ok(());
        }
        let x = value
            .as_float()
            .map_err(|_| GqlError::type_error("STDDEV requires numeric values"))?;
        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
        Ok(())
    }
    fn finish(&self) -> GqlValue {
        if self.count == 0 {
            return GqlValue::Null;
        }
        if self.sample && self.count < 2 {
            return GqlValue::Null;
        }
        let variance = if self.sample {
            self.m2 / (self.count - 1) as f64
        } else {
            self.m2 / self.count as f64
        };
        GqlValue::Float(variance.sqrt())
    }
}

// ── Horizontal aggregate (within a single binding over a List) ─────

/// Evaluate an aggregate over a list of values within a single binding.
pub(crate) fn eval_horizontal_aggregate(
    op: AggregateOp,
    elements: &[GqlValue],
) -> Result<GqlValue, GqlError> {
    match op {
        AggregateOp::Count => Ok(GqlValue::Int(
            elements.iter().filter(|v| !v.is_null()).count() as i64,
        )),
        AggregateOp::Sum => {
            let mut sum_int: i64 = 0;
            let mut sum_float: f64 = 0.0;
            let mut has_float = false;
            let mut has_value = false;
            for val in elements {
                if val.is_null() {
                    continue;
                }
                has_value = true;
                match val {
                    GqlValue::Int(i) => {
                        sum_int = sum_int
                            .checked_add(*i)
                            .ok_or_else(|| GqlError::type_error("SUM integer overflow"))?;
                    }
                    GqlValue::UInt(u) => {
                        let i = i64::try_from(*u).map_err(|_| {
                            GqlError::type_error("SUM: UINT64 value exceeds INT64 range")
                        })?;
                        sum_int = sum_int
                            .checked_add(i)
                            .ok_or_else(|| GqlError::type_error("SUM integer overflow"))?;
                    }
                    GqlValue::Float(f) => {
                        has_float = true;
                        sum_float += f;
                    }
                    _ => return Err(GqlError::type_error("SUM requires numeric values")),
                }
            }
            if !has_value {
                return Ok(GqlValue::Null);
            }
            if has_float {
                Ok(GqlValue::Float(sum_float + sum_int as f64))
            } else {
                Ok(GqlValue::Int(sum_int))
            }
        }
        AggregateOp::Avg => {
            let mut sum: f64 = 0.0;
            let mut count: usize = 0;
            for val in elements {
                if val.is_null() {
                    continue;
                }
                sum += val.as_float()?;
                count += 1;
            }
            if count == 0 {
                Ok(GqlValue::Null)
            } else {
                Ok(GqlValue::Float(sum / count as f64))
            }
        }
        AggregateOp::Min | AggregateOp::Max => {
            let is_max = op == AggregateOp::Max;
            let mut best: Option<GqlValue> = None;
            for val in elements {
                if val.is_null() {
                    continue;
                }
                best = Some(match best {
                    None => val.clone(),
                    Some(current) => {
                        let ord = val.sort_order(&current);
                        if (is_max && ord == std::cmp::Ordering::Greater)
                            || (!is_max && ord == std::cmp::Ordering::Less)
                        {
                            val.clone()
                        } else {
                            current
                        }
                    }
                });
            }
            Ok(best.unwrap_or(GqlValue::Null))
        }
        AggregateOp::CollectList => {
            let filtered: Vec<GqlValue> =
                elements.iter().filter(|v| !v.is_null()).cloned().collect();
            let element_type = crate::types::value::infer_list_element_type(&filtered);
            Ok(GqlValue::List(GqlList {
                element_type,
                elements: Arc::from(filtered),
            }))
        }
        AggregateOp::StddevSamp | AggregateOp::StddevPop => {
            let sample = op == AggregateOp::StddevSamp;
            let mut count = 0u64;
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            for val in elements {
                if val.is_null() {
                    continue;
                }
                let x = val.as_float()?;
                count += 1;
                let delta = x - mean;
                mean += delta / count as f64;
                let delta2 = x - mean;
                m2 += delta * delta2;
            }
            if count == 0 {
                return Ok(GqlValue::Null);
            }
            if sample && count < 2 {
                return Ok(GqlValue::Null);
            }
            let variance = if sample {
                m2 / (count - 1) as f64
            } else {
                m2 / count as f64
            };
            Ok(GqlValue::Float(variance.sqrt()))
        }
    }
}

// ── Aggregate evaluation (for RETURN stage) ────────────────────────

/// Evaluate an aggregate expression over a set of bindings (vertical aggregation).
/// Uses the Accumulator trait for extensibility.
pub fn eval_aggregate(
    agg: &AggregateExpr,
    bindings: &[Binding],
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    // count(*) special case: no expression to evaluate
    if agg.op == AggregateOp::Count && agg.expr.is_none() {
        return Ok(GqlValue::Int(bindings.len() as i64));
    }
    let expr = agg.expr.as_ref().ok_or_else(|| GqlError::InvalidArgument {
        message: format!("{:?} requires an argument", agg.op),
    })?;

    let mut acc = create_accumulator(agg.op);
    if agg.distinct {
        // Collect values, deduplicate using distinctness_key, then accumulate
        let mut seen = std::collections::HashSet::new();
        for binding in bindings {
            let val = eval_expr_ctx(expr, binding, ctx)?;
            let key = val.distinctness_key();
            if seen.insert(key) {
                acc.accumulate(&val)?;
            }
        }
    } else {
        for binding in bindings {
            let val = eval_expr_ctx(expr, binding, ctx)?;
            acc.accumulate(&val)?;
        }
    }
    Ok(acc.finish())
}
