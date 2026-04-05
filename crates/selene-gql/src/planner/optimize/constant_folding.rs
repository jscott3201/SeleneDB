//! Constant folding optimization rule.
//!
//! Evaluates constant expressions at compile time (e.g. `70 + 2` becomes `72`,
//! `true AND x` becomes `x`). Reduces runtime evaluation overhead for
//! expressions that can be resolved statically.

use std::borrow::Cow;

use crate::ast::expr::{ArithOp, CompareOp, Expr, LogicOp};
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

#[derive(Debug)]
pub struct ConstantFoldingRule;

impl GqlOptimizerRule for ConstantFoldingRule {
    fn name(&self) -> &'static str {
        "ConstantFolding"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        let mut changed = false;
        for op in &mut plan.pipeline {
            match op {
                PipelineOp::Filter { predicate } => {
                    let (new_expr, folded) = fold_expr(predicate);
                    if folded {
                        *predicate = new_expr.into_owned();
                        changed = true;
                    }
                }
                PipelineOp::Let { bindings } => {
                    for (_, expr) in bindings {
                        let (new_expr, folded) = fold_expr(expr);
                        if folded {
                            *expr = new_expr.into_owned();
                            changed = true;
                        }
                    }
                }
                PipelineOp::Return {
                    projections,
                    having,
                    ..
                }
                | PipelineOp::With {
                    projections,
                    having,
                    ..
                } => {
                    for proj in projections {
                        let (new_expr, folded) = fold_expr(&proj.expr);
                        if folded {
                            proj.expr = new_expr.into_owned();
                            changed = true;
                        }
                    }
                    if let Some(h) = having {
                        let (new_expr, folded) = fold_expr(h);
                        if folded {
                            *h = new_expr.into_owned();
                            changed = true;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(if changed {
            Transformed::yes(plan)
        } else {
            Transformed::no(plan)
        })
    }
}

fn fold_expr(expr: &Expr) -> (Cow<'_, Expr>, bool) {
    match expr {
        Expr::Arithmetic(left, op, right) => {
            let (l, lc) = fold_expr(left);
            let (r, rc) = fold_expr(right);
            if let (Expr::Literal(lv), Expr::Literal(rv)) = (l.as_ref(), r.as_ref())
                && let Some(result) = fold_arithmetic(lv, *op, rv)
            {
                return (Cow::Owned(Expr::Literal(result)), true);
            }
            if lc || rc {
                (
                    Cow::Owned(Expr::Arithmetic(
                        Box::new(l.into_owned()),
                        *op,
                        Box::new(r.into_owned()),
                    )),
                    true,
                )
            } else {
                (Cow::Borrowed(expr), false)
            }
        }
        Expr::Logic(left, op, right) => {
            let (l, lc) = fold_expr(left);
            let (r, rc) = fold_expr(right);
            if *op == LogicOp::And {
                if matches!(l.as_ref(), Expr::Literal(GqlValue::Bool(true))) {
                    return (Cow::Owned(r.into_owned()), true);
                }
                if matches!(l.as_ref(), Expr::Literal(GqlValue::Bool(false))) {
                    return (Cow::Owned(Expr::Literal(GqlValue::Bool(false))), true);
                }
                if matches!(r.as_ref(), Expr::Literal(GqlValue::Bool(true))) {
                    return (Cow::Owned(l.into_owned()), true);
                }
                if matches!(r.as_ref(), Expr::Literal(GqlValue::Bool(false))) {
                    return (Cow::Owned(Expr::Literal(GqlValue::Bool(false))), true);
                }
            }
            if *op == LogicOp::Or {
                if matches!(l.as_ref(), Expr::Literal(GqlValue::Bool(false))) {
                    return (Cow::Owned(r.into_owned()), true);
                }
                if matches!(l.as_ref(), Expr::Literal(GqlValue::Bool(true))) {
                    return (Cow::Owned(Expr::Literal(GqlValue::Bool(true))), true);
                }
                if matches!(r.as_ref(), Expr::Literal(GqlValue::Bool(false))) {
                    return (Cow::Owned(l.into_owned()), true);
                }
                if matches!(r.as_ref(), Expr::Literal(GqlValue::Bool(true))) {
                    return (Cow::Owned(Expr::Literal(GqlValue::Bool(true))), true);
                }
            }
            if lc || rc {
                (
                    Cow::Owned(Expr::Logic(
                        Box::new(l.into_owned()),
                        *op,
                        Box::new(r.into_owned()),
                    )),
                    true,
                )
            } else {
                (Cow::Borrowed(expr), false)
            }
        }
        Expr::Not(inner) => {
            let (i, ic) = fold_expr(inner);
            if let Expr::Not(inner2) = i.as_ref() {
                return (Cow::Owned(inner2.as_ref().clone()), true);
            }
            if let Expr::Literal(GqlValue::Bool(b)) = i.as_ref() {
                return (Cow::Owned(Expr::Literal(GqlValue::Bool(!b))), true);
            }
            if ic {
                (Cow::Owned(Expr::Not(Box::new(i.into_owned()))), true)
            } else {
                (Cow::Borrowed(expr), false)
            }
        }
        Expr::Negate(inner) => {
            let (i, ic) = fold_expr(inner);
            match i.as_ref() {
                Expr::Literal(GqlValue::Int(n)) => {
                    if let Some(neg) = n.checked_neg() {
                        return (Cow::Owned(Expr::Literal(GqlValue::Int(neg))), true);
                    }
                    // i64::MIN cannot be negated -- leave as runtime Negate
                }
                Expr::Literal(GqlValue::Float(f)) => {
                    return (Cow::Owned(Expr::Literal(GqlValue::Float(-f))), true);
                }
                _ => {}
            }
            if ic {
                (Cow::Owned(Expr::Negate(Box::new(i.into_owned()))), true)
            } else {
                (Cow::Borrowed(expr), false)
            }
        }
        Expr::Compare(left, op, right) => {
            let (l, lc) = fold_expr(left);
            let (r, rc) = fold_expr(right);
            if let (Expr::Literal(lv), Expr::Literal(rv)) = (l.as_ref(), r.as_ref())
                && let Some(result) = fold_compare(lv, *op, rv)
            {
                return (Cow::Owned(Expr::Literal(GqlValue::Bool(result))), true);
            }
            if lc || rc {
                (
                    Cow::Owned(Expr::Compare(
                        Box::new(l.into_owned()),
                        *op,
                        Box::new(r.into_owned()),
                    )),
                    true,
                )
            } else {
                (Cow::Borrowed(expr), false)
            }
        }
        Expr::Concat(left, right) => {
            let (l, lc) = fold_expr(left);
            let (r, rc) = fold_expr(right);
            if let (Expr::Literal(GqlValue::String(ls)), Expr::Literal(GqlValue::String(rs))) =
                (l.as_ref(), r.as_ref())
            {
                let combined = format!("{ls}{rs}");
                return (
                    Cow::Owned(Expr::Literal(GqlValue::String(smol_str::SmolStr::new(
                        &combined,
                    )))),
                    true,
                );
            }
            if lc || rc {
                (
                    Cow::Owned(Expr::Concat(
                        Box::new(l.into_owned()),
                        Box::new(r.into_owned()),
                    )),
                    true,
                )
            } else {
                (Cow::Borrowed(expr), false)
            }
        }
        _ => (Cow::Borrowed(expr), false),
    }
}

fn fold_arithmetic(left: &GqlValue, op: ArithOp, right: &GqlValue) -> Option<GqlValue> {
    // Int+Int: use checked integer arithmetic to preserve precision.
    if let (GqlValue::Int(a), GqlValue::Int(b)) = (left, right) {
        return match op {
            ArithOp::Add => a.checked_add(*b).map(GqlValue::Int),
            ArithOp::Sub => a.checked_sub(*b).map(GqlValue::Int),
            ArithOp::Mul => a.checked_mul(*b).map(GqlValue::Int),
            ArithOp::Div if *b != 0 => Some(GqlValue::Int(a / b)),
            ArithOp::Mod if *b != 0 => a.checked_rem(*b).map(GqlValue::Int),
            _ => None,
        };
    }

    // Mixed or Float+Float: promote both to f64.
    let a = match left {
        GqlValue::Int(v) => *v as f64,
        GqlValue::Float(v) => *v,
        _ => return None,
    };
    let b = match right {
        GqlValue::Int(v) => *v as f64,
        GqlValue::Float(v) => *v,
        _ => return None,
    };
    Some(match op {
        ArithOp::Add => GqlValue::Float(a + b),
        ArithOp::Sub => GqlValue::Float(a - b),
        ArithOp::Mul => GqlValue::Float(a * b),
        ArithOp::Div if b != 0.0 => GqlValue::Float(a / b),
        ArithOp::Mod if b != 0.0 => GqlValue::Float(a % b),
        _ => return None,
    })
}

fn fold_compare(left: &GqlValue, op: CompareOp, right: &GqlValue) -> Option<bool> {
    match (left, right) {
        (GqlValue::Int(a), GqlValue::Int(b)) => Some(cmp_i64(*a, op, *b)),
        (GqlValue::Float(a), GqlValue::Float(b)) => Some(cmp_f64(*a, op, *b)),
        (GqlValue::Int(a), GqlValue::Float(b)) => Some(cmp_f64(*a as f64, op, *b)),
        (GqlValue::Float(a), GqlValue::Int(b)) => Some(cmp_f64(*a, op, *b as f64)),
        (GqlValue::Bool(a), GqlValue::Bool(b)) => match op {
            CompareOp::Eq => Some(a == b),
            CompareOp::Neq => Some(a != b),
            _ => None,
        },
        _ => None,
    }
}

fn cmp_i64(a: i64, op: CompareOp, b: i64) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Neq => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Gt => a > b,
        CompareOp::Lte => a <= b,
        CompareOp::Gte => a >= b,
    }
}

fn cmp_f64(a: f64, op: CompareOp, b: f64) -> bool {
    match op {
        // Bit-exact comparison: avoids epsilon tolerance which can give wrong
        // results for values near the boundary (e.g. 0.1 + 0.2 != 0.3).
        // The optimizer only compares literal constants from the AST, so
        // bit-exact equality is the correct semantics here.
        CompareOp::Eq => a.to_bits() == b.to_bits(),
        CompareOp::Neq => a.to_bits() != b.to_bits(),
        CompareOp::Lt => a < b,
        CompareOp::Gt => a > b,
        CompareOp::Lte => a <= b,
        CompareOp::Gte => a >= b,
    }
}
