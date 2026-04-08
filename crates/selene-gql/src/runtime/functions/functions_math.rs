//! Math scalar functions: abs, ceil, floor, round, sqrt, sign,
//! power, log, exp, trig, and related functions.

use super::{EvalContext, GqlError, GqlValue, ScalarFunction};

// ── Primary math functions ──────────────────────────────────────────

macro_rules! num_fn {
    ($name:ident, $fn_name:expr, $desc:expr, $body:expr) => {
        pub(crate) struct $name;
        impl ScalarFunction for $name {
            fn name(&self) -> &'static str {
                $fn_name
            }
            fn description(&self) -> &'static str {
                $desc
            }
            fn invoke(
                &self,
                args: &[GqlValue],
                _ctx: &EvalContext<'_>,
            ) -> Result<GqlValue, GqlError> {
                match args.first() {
                    Some(GqlValue::Null) => Ok(GqlValue::Null),
                    Some(v) => ($body)(v),
                    None => Err(GqlError::InvalidArgument {
                        message: format!("{} requires an argument", $fn_name),
                    }),
                }
            }
        }
    };
}

num_fn!(
    AbsFunction,
    "abs",
    "Absolute value",
    |v: &GqlValue| match v {
        GqlValue::Int(i) =>
            Ok(GqlValue::Int(i.checked_abs().ok_or_else(|| {
                GqlError::type_error("abs: integer overflow (i64::MIN)")
            })?)),
        GqlValue::Float(f) => Ok(GqlValue::Float(f.abs())),
        _ => Err(GqlError::type_error("abs() requires a number")),
    }
);

num_fn!(
    CeilFunction,
    "ceil",
    "Round up to nearest integer",
    |v: &GqlValue| match v {
        GqlValue::Float(f) => Ok(GqlValue::Int(f.ceil() as i64)),
        GqlValue::Int(i) => Ok(GqlValue::Int(*i)),
        _ => Err(GqlError::type_error("ceil() requires a number")),
    }
);

num_fn!(
    FloorFunction,
    "floor",
    "Round down to nearest integer",
    |v: &GqlValue| match v {
        GqlValue::Float(f) => Ok(GqlValue::Int(f.floor() as i64)),
        GqlValue::Int(i) => Ok(GqlValue::Int(*i)),
        _ => Err(GqlError::type_error("floor() requires a number")),
    }
);

num_fn!(
    SqrtFunction,
    "sqrt",
    "Square root",
    |v: &GqlValue| match v {
        GqlValue::Float(f) if *f < 0.0 => Ok(GqlValue::Null),
        GqlValue::Float(f) => Ok(GqlValue::Float(f.sqrt())),
        GqlValue::Int(i) if *i < 0 => Ok(GqlValue::Null),
        GqlValue::Int(i) => Ok(GqlValue::Float((*i as f64).sqrt())),
        _ => Err(GqlError::type_error("sqrt() requires a number")),
    }
);

num_fn!(
    SignFunction,
    "sign",
    "Returns -1, 0, or 1",
    |v: &GqlValue| match v {
        GqlValue::Int(i) => Ok(GqlValue::Int(i.signum())),
        GqlValue::Float(f) => Ok(GqlValue::Int(if *f < 0.0 {
            -1
        } else {
            i64::from(*f > 0.0)
        })),
        _ => Err(GqlError::type_error("sign() requires a number")),
    }
);

pub(crate) struct RoundFunction;
impl ScalarFunction for RoundFunction {
    fn name(&self) -> &'static str {
        "round"
    }
    fn description(&self) -> &'static str {
        "Round to nearest integer (or N decimal places)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let val = args.first().ok_or_else(|| GqlError::InvalidArgument {
            message: "round() requires an argument".into(),
        })?;
        let places = args
            .get(1)
            .and_then(|v| {
                if let GqlValue::Int(n) = v {
                    Some(*n)
                } else {
                    None
                }
            })
            .unwrap_or(0);
        match val {
            GqlValue::Float(f) => {
                let factor = 10f64.powi(places as i32);
                Ok(GqlValue::Float((f * factor).round() / factor))
            }
            GqlValue::Int(i) => Ok(GqlValue::Int(*i)),
            GqlValue::Null => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("round() requires a number")),
        }
    }
}

// ── Extended math functions ─────────────────────────────────────────

pub(crate) struct PowerFn;
impl ScalarFunction for PowerFn {
    fn name(&self) -> &'static str {
        "power"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let base = match args.first() {
            Some(GqlValue::Float(f)) => *f,
            Some(GqlValue::Int(i)) => *i as f64,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("power() requires numbers")),
        };
        let exp = match args.get(1) {
            Some(GqlValue::Float(f)) => *f,
            Some(GqlValue::Int(i)) => *i as f64,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("power() requires numbers")),
        };
        Ok(GqlValue::Float(base.powf(exp)))
    }
}

num_fn!(
    LogFunction,
    "log",
    "Natural logarithm",
    |v: &GqlValue| match v {
        GqlValue::Float(f) if *f > 0.0 => Ok(GqlValue::Float(f.ln())),
        GqlValue::Float(f) if *f == 0.0 => Ok(GqlValue::Null), // log(0) -> NULL
        GqlValue::Float(_) => Ok(GqlValue::Null),
        GqlValue::Int(i) if *i > 0 => Ok(GqlValue::Float((*i as f64).ln())),
        GqlValue::Int(0) => Ok(GqlValue::Null),
        GqlValue::Int(_) => Ok(GqlValue::Null),
        _ => Err(GqlError::type_error("log() requires a number")),
    }
);

num_fn!(
    Log10Function,
    "log10",
    "Base-10 logarithm",
    |v: &GqlValue| match v {
        GqlValue::Float(f) if *f > 0.0 => Ok(GqlValue::Float(f.log10())),
        GqlValue::Float(f) if *f == 0.0 => Ok(GqlValue::Null),
        GqlValue::Float(_) => Ok(GqlValue::Null),
        GqlValue::Int(i) if *i > 0 => Ok(GqlValue::Float((*i as f64).log10())),
        GqlValue::Int(0) => Ok(GqlValue::Null),
        GqlValue::Int(_) => Ok(GqlValue::Null),
        _ => Err(GqlError::type_error("log10() requires a number")),
    }
);

num_fn!(
    ExpFunction,
    "exp",
    "Euler's number raised to power",
    |v: &GqlValue| match v {
        GqlValue::Float(f) => Ok(GqlValue::Float(f.exp())),
        GqlValue::Int(i) => Ok(GqlValue::Float((*i as f64).exp())),
        _ => Err(GqlError::type_error("exp() requires a number")),
    }
);

num_fn!(
    SinFunction,
    "sin",
    "Sine (radians)",
    |v: &GqlValue| match v {
        GqlValue::Float(f) => Ok(GqlValue::Float(f.sin())),
        GqlValue::Int(i) => Ok(GqlValue::Float((*i as f64).sin())),
        _ => Err(GqlError::type_error("sin() requires a number")),
    }
);

num_fn!(
    CosFunction,
    "cos",
    "Cosine (radians)",
    |v: &GqlValue| match v {
        GqlValue::Float(f) => Ok(GqlValue::Float(f.cos())),
        GqlValue::Int(i) => Ok(GqlValue::Float((*i as f64).cos())),
        _ => Err(GqlError::type_error("cos() requires a number")),
    }
);

num_fn!(
    TanFunction,
    "tan",
    "Tangent (radians)",
    |v: &GqlValue| match v {
        GqlValue::Float(f) => Ok(GqlValue::Float(f.tan())),
        GqlValue::Int(i) => Ok(GqlValue::Float((*i as f64).tan())),
        _ => Err(GqlError::type_error("tan() requires a number")),
    }
);

pub(crate) struct PiFunction;
impl ScalarFunction for PiFunction {
    fn name(&self) -> &'static str {
        "pi"
    }
    fn invoke(&self, _args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        Ok(GqlValue::Float(std::f64::consts::PI))
    }
}

pub(crate) struct ModFunction;
impl ScalarFunction for ModFunction {
    fn name(&self) -> &'static str {
        "mod"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a = match args.first() {
            Some(GqlValue::Int(i)) => *i,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("mod() requires integers")),
        };
        let b = match args.get(1) {
            Some(GqlValue::Int(i)) if *i != 0 => *i,
            Some(GqlValue::Int(0)) => return Err(GqlError::type_error("mod() division by zero")),
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("mod() requires integers")),
        };
        Ok(GqlValue::Int(a % b))
    }
}

// ── Additional math functions (GF02-GF03) ──────────────────────────

macro_rules! unary_math_fn {
    ($struct_name:ident, $fn_name:expr, $method:ident, $desc:expr) => {
        pub(crate) struct $struct_name;
        impl ScalarFunction for $struct_name {
            fn name(&self) -> &'static str {
                $fn_name
            }
            fn description(&self) -> &'static str {
                $desc
            }
            fn invoke(
                &self,
                args: &[GqlValue],
                _ctx: &EvalContext<'_>,
            ) -> Result<GqlValue, GqlError> {
                match args.first() {
                    Some(GqlValue::Null) | None => Ok(GqlValue::Null),
                    Some(v) => Ok(GqlValue::Float(v.as_float()?.$method())),
                }
            }
        }
    };
}

num_fn!(
    LnFunction,
    "ln",
    "Natural logarithm",
    |v: &GqlValue| match v {
        GqlValue::Float(f) if *f > 0.0 => Ok(GqlValue::Float(f.ln())),
        GqlValue::Float(f) if *f == 0.0 => Ok(GqlValue::Null),
        GqlValue::Float(_) => Ok(GqlValue::Null),
        GqlValue::Int(i) if *i > 0 => Ok(GqlValue::Float((*i as f64).ln())),
        GqlValue::Int(0) => Ok(GqlValue::Null),
        GqlValue::Int(_) => Ok(GqlValue::Null),
        _ => Err(GqlError::type_error("ln() requires a number")),
    }
);
pub(crate) struct CotFunction; // cot(x) = 1/tan(x) -- custom impl below
unary_math_fn!(SinhFunction, "sinh", sinh, "Hyperbolic sine");
unary_math_fn!(CoshFunction, "cosh", cosh, "Hyperbolic cosine");
unary_math_fn!(TanhFunction, "tanh", tanh, "Hyperbolic tangent");
unary_math_fn!(AsinFunction, "asin", asin, "Inverse sine");
unary_math_fn!(AcosFunction, "acos", acos, "Inverse cosine");
unary_math_fn!(AtanFunction, "atan", atan, "Inverse tangent");
unary_math_fn!(DegreesFunction, "degrees", to_degrees, "Radians to degrees");
unary_math_fn!(RadiansFunction, "radians", to_radians, "Degrees to radians");

// cot(x) = 1/tan(x), override the macro since .recip() isn't quite right
impl ScalarFunction for CotFunction {
    fn name(&self) -> &'static str {
        "cot"
    }
    fn description(&self) -> &'static str {
        "Cotangent"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(v) => {
                let x = v.as_float()?;
                let t = x.tan();
                if t == 0.0 {
                    return Err(GqlError::type_error("cot: tan(x) = 0"));
                }
                Ok(GqlValue::Float(1.0 / t))
            }
        }
    }
}

pub(crate) struct Atan2Function;
impl ScalarFunction for Atan2Function {
    fn name(&self) -> &'static str {
        "atan2"
    }
    fn description(&self) -> &'static str {
        "Two-argument arctangent"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let y = args
            .first()
            .map(|v| v.as_float())
            .transpose()?
            .unwrap_or(0.0);
        let x = args
            .get(1)
            .map(|v| v.as_float())
            .transpose()?
            .unwrap_or(0.0);
        Ok(GqlValue::Float(y.atan2(x)))
    }
}

pub(crate) struct CardinalityFunction;
impl ScalarFunction for CardinalityFunction {
    fn name(&self) -> &'static str {
        "cardinality"
    }
    fn description(&self) -> &'static str {
        "Cardinality (length) of a list"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::List(l)) => Ok(GqlValue::Int(l.len() as i64)),
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("cardinality() requires a list")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::functions::FunctionRegistry;
    use crate::types::value::{GqlList, GqlType};
    use selene_graph::SeleneGraph;
    use std::sync::Arc;

    fn ctx() -> (SeleneGraph, FunctionRegistry) {
        (SeleneGraph::new(), FunctionRegistry::with_builtins())
    }

    // ── AbsFunction ──

    #[test]
    fn abs_positive_int() {
        let f = AbsFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(42)], &c).unwrap(),
            GqlValue::Int(42)
        );
    }

    #[test]
    fn abs_negative_int() {
        let f = AbsFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(-7)], &c).unwrap(),
            GqlValue::Int(7)
        );
    }

    #[test]
    fn abs_negative_float() {
        let f = AbsFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(-3.5)], &c).unwrap(),
            GqlValue::Float(3.5)
        );
    }

    #[test]
    fn abs_i64_min_overflows() {
        let f = AbsFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // i64::MIN has no positive representation in i64
        assert!(f.invoke(&[GqlValue::Int(i64::MIN)], &c).is_err());
    }

    #[test]
    fn abs_null_returns_null() {
        let f = AbsFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    // ── CeilFunction / FloorFunction ──

    #[test]
    fn ceil_positive_float() {
        let f = CeilFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(2.3)], &c).unwrap(),
            GqlValue::Int(3)
        );
    }

    #[test]
    fn ceil_negative_float() {
        let f = CeilFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(-2.3)], &c).unwrap(),
            GqlValue::Int(-2)
        );
    }

    #[test]
    fn ceil_int_passthrough() {
        let f = CeilFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Int(5)], &c).unwrap(), GqlValue::Int(5));
    }

    #[test]
    fn floor_positive_float() {
        let f = FloorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(2.7)], &c).unwrap(),
            GqlValue::Int(2)
        );
    }

    #[test]
    fn floor_negative_float() {
        let f = FloorFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(-2.3)], &c).unwrap(),
            GqlValue::Int(-3)
        );
    }

    // ── RoundFunction ──

    #[test]
    fn round_to_nearest_int() {
        let f = RoundFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(2.5)], &c).unwrap(),
            GqlValue::Float(3.0)
        );
    }

    #[test]
    fn round_with_decimal_places() {
        let f = RoundFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::Float(3.15159), GqlValue::Int(2)], &c)
            .unwrap();
        if let GqlValue::Float(v) = r {
            assert!((v - 3.15).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn round_null_returns_null() {
        let f = RoundFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    // ── SqrtFunction ──

    #[test]
    fn sqrt_of_int() {
        let f = SqrtFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[GqlValue::Int(16)], &c).unwrap();
        assert_eq!(r, GqlValue::Float(4.0));
    }

    #[test]
    fn sqrt_of_float() {
        let f = SqrtFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(2.0)], &c).unwrap() {
            assert!((v - std::f64::consts::SQRT_2).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn sqrt_negative_returns_null() {
        let f = SqrtFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // sqrt of negative returns Null (consistent with log/log10 domain guards)
        assert_eq!(f.invoke(&[GqlValue::Int(-1)], &c).unwrap(), GqlValue::Null);
        assert_eq!(
            f.invoke(&[GqlValue::Float(-2.5)], &c).unwrap(),
            GqlValue::Null
        );
    }

    // ── SignFunction ──

    #[test]
    fn sign_positive() {
        let f = SignFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(42)], &c).unwrap(),
            GqlValue::Int(1)
        );
    }

    #[test]
    fn sign_negative() {
        let f = SignFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(-5)], &c).unwrap(),
            GqlValue::Int(-1)
        );
    }

    #[test]
    fn sign_zero() {
        let f = SignFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Int(0)], &c).unwrap(), GqlValue::Int(0));
    }

    #[test]
    fn sign_float_negative() {
        let f = SignFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(-0.5)], &c).unwrap(),
            GqlValue::Int(-1)
        );
    }

    // ── PowerFn ──

    #[test]
    fn power_basic() {
        let f = PowerFn;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(2), GqlValue::Int(3)], &c).unwrap(),
            GqlValue::Float(8.0)
        );
    }

    #[test]
    fn power_zero_zero() {
        let f = PowerFn;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // 0^0 = 1.0 per IEEE 754
        assert_eq!(
            f.invoke(&[GqlValue::Int(0), GqlValue::Int(0)], &c).unwrap(),
            GqlValue::Float(1.0)
        );
    }

    #[test]
    fn power_negative_exponent() {
        let f = PowerFn;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f
            .invoke(&[GqlValue::Int(2), GqlValue::Int(-1)], &c)
            .unwrap()
        {
            assert!((v - 0.5).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn power_null_returns_null() {
        let f = PowerFn;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Null, GqlValue::Int(2)], &c).unwrap(),
            GqlValue::Null
        );
    }

    // ── LogFunction / Log10Function / LnFunction ──

    #[test]
    fn log_positive() {
        let f = LogFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f
            .invoke(&[GqlValue::Float(std::f64::consts::E)], &c)
            .unwrap()
        {
            assert!((v - 1.0).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn log_zero_returns_null() {
        let f = LogFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(0.0)], &c).unwrap(),
            GqlValue::Null
        );
    }

    #[test]
    fn log_negative_returns_null() {
        let f = LogFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(-1.0)], &c).unwrap(),
            GqlValue::Null
        );
        assert_eq!(f.invoke(&[GqlValue::Int(-5)], &c).unwrap(), GqlValue::Null);
    }

    #[test]
    fn log10_hundred() {
        let f = Log10Function;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Int(100)], &c).unwrap() {
            assert!((v - 2.0).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn log10_zero_returns_null() {
        let f = Log10Function;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Int(0)], &c).unwrap(), GqlValue::Null);
    }

    #[test]
    fn log10_negative_returns_null() {
        let f = Log10Function;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(-1.0)], &c).unwrap(),
            GqlValue::Null
        );
        assert_eq!(f.invoke(&[GqlValue::Int(-5)], &c).unwrap(), GqlValue::Null);
    }

    #[test]
    fn ln_positive() {
        let f = LnFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(1.0)], &c).unwrap() {
            assert!((v - 0.0).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn ln_zero_returns_null() {
        let f = LnFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Int(0)], &c).unwrap(), GqlValue::Null);
        assert_eq!(
            f.invoke(&[GqlValue::Float(0.0)], &c).unwrap(),
            GqlValue::Null
        );
    }

    #[test]
    fn ln_negative_returns_null() {
        let f = LnFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Float(-1.0)], &c).unwrap(),
            GqlValue::Null
        );
        assert_eq!(f.invoke(&[GqlValue::Int(-5)], &c).unwrap(), GqlValue::Null);
    }

    // ── ExpFunction ──

    #[test]
    fn exp_zero() {
        let f = ExpFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(0)], &c).unwrap(),
            GqlValue::Float(1.0)
        );
    }

    #[test]
    fn exp_one() {
        let f = ExpFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Int(1)], &c).unwrap() {
            assert!((v - std::f64::consts::E).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    // ── ModFunction ──

    #[test]
    fn mod_basic() {
        let f = ModFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(10), GqlValue::Int(3)], &c)
                .unwrap(),
            GqlValue::Int(1)
        );
    }

    #[test]
    fn mod_division_by_zero_errors() {
        let f = ModFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert!(
            f.invoke(&[GqlValue::Int(10), GqlValue::Int(0)], &c)
                .is_err()
        );
    }

    #[test]
    fn mod_null_returns_null() {
        let f = ModFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Null, GqlValue::Int(3)], &c).unwrap(),
            GqlValue::Null
        );
    }

    // ── PiFunction ──

    #[test]
    fn pi_returns_pi() {
        let f = PiFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[], &c).unwrap(),
            GqlValue::Float(std::f64::consts::PI)
        );
    }

    // ── Trig functions ──

    #[test]
    fn sin_zero() {
        let f = SinFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(0.0)], &c).unwrap() {
            assert!(v.abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn cos_zero() {
        let f = CosFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(0.0)], &c).unwrap() {
            assert!((v - 1.0).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn tan_zero() {
        let f = TanFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(0.0)], &c).unwrap() {
            assert!(v.abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    // ── CotFunction ──

    #[test]
    fn cot_pi_over_4() {
        let f = CotFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // cot(pi/4) = 1/tan(pi/4) = 1.0
        if let GqlValue::Float(v) = f
            .invoke(&[GqlValue::Float(std::f64::consts::FRAC_PI_4)], &c)
            .unwrap()
        {
            assert!((v - 1.0).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    // ── Atan2Function ──

    #[test]
    fn atan2_basic() {
        let f = Atan2Function;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f
            .invoke(&[GqlValue::Float(1.0), GqlValue::Float(1.0)], &c)
            .unwrap()
        {
            assert!((v - std::f64::consts::FRAC_PI_4).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    // ── DegreesFunction / RadiansFunction ──

    #[test]
    fn degrees_pi_is_180() {
        let f = DegreesFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f
            .invoke(&[GqlValue::Float(std::f64::consts::PI)], &c)
            .unwrap()
        {
            assert!((v - 180.0).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn radians_180_is_pi() {
        let f = RadiansFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(180.0)], &c).unwrap() {
            assert!((v - std::f64::consts::PI).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    // ── CardinalityFunction ──

    #[test]
    fn cardinality_of_list() {
        let f = CardinalityFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let list = GqlValue::List(GqlList {
            element_type: GqlType::Int,
            elements: Arc::from(vec![GqlValue::Int(1), GqlValue::Int(2)]),
        });
        assert_eq!(f.invoke(&[list], &c).unwrap(), GqlValue::Int(2));
    }

    #[test]
    fn cardinality_null_returns_null() {
        let f = CardinalityFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    #[test]
    fn cardinality_wrong_type_errors() {
        let f = CardinalityFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert!(f.invoke(&[GqlValue::Int(5)], &c).is_err());
    }

    // ── Hyperbolic functions ──

    #[test]
    fn sinh_zero() {
        let f = SinhFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(0.0)], &c).unwrap() {
            assert!(v.abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn cosh_zero() {
        let f = CoshFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(0.0)], &c).unwrap() {
            assert!((v - 1.0).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn tanh_zero() {
        let f = TanhFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(0.0)], &c).unwrap() {
            assert!(v.abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    // ── Inverse trig ──

    #[test]
    fn asin_zero() {
        let f = AsinFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(0.0)], &c).unwrap() {
            assert!(v.abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn acos_one() {
        let f = AcosFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(1.0)], &c).unwrap() {
            assert!(v.abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn atan_zero() {
        let f = AtanFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        if let GqlValue::Float(v) = f.invoke(&[GqlValue::Float(0.0)], &c).unwrap() {
            assert!(v.abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }
}
