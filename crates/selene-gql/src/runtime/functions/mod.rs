//! Function registry: trait-based scalar function registration.
//!
//! Provides a registry enabling user-defined functions and plan-time
//! signature validation.

mod functions_core;
mod functions_math;
mod functions_string;
mod functions_temporal;

use std::collections::HashMap;
use std::sync::Arc;

use selene_core::IStr;

use crate::runtime::eval::EvalContext;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue, ZonedDateTime};

// Re-export parse_iso8601 (used by eval.rs and procedures/history.rs)
pub use functions_core::parse_iso8601;

// Re-export sub-module structs for with_builtins() registration
use functions_core::{
    CharLengthFunction, CoalesceFunction, DegreeFunction, DurationFunction, EdgesFunction,
    ElementIdFunction, EndNodeFunction, IdFunction, InDegreeFunction, IsAcyclicFunction,
    IsSimpleFunction, IsTrailFunction, LowerFunction, NodesFunction, OutDegreeFunction,
    PathLengthFunction, PropertiesFunction, PropertyNamesFunction, SizeFunction, StartNodeFunction,
    TrimFunction, TypeFunction, UpperFunction, ZonedDatetimeFunction,
};
use functions_math::{
    AbsFunction, AcosFunction, AsinFunction, Atan2Function, AtanFunction, CardinalityFunction,
    CeilFunction, CosFunction, CoshFunction, CotFunction, DegreesFunction, ExpFunction,
    FloorFunction, LnFunction, Log10Function, LogFunction, ModFunction, PiFunction, PowerFn,
    RadiansFunction, RoundFunction, SignFunction, SinFunction, SinhFunction, SqrtFunction,
    TanFunction, TanhFunction,
};
use functions_string::{
    ContainsFn, DoubleFunction, EndsWithFunction, HeadFunction, KeysFunction, LastFunction,
    LeftFunction, LengthFunction, ListAppendFunction, ListContainsFunction, ListLengthFunction,
    ListPrependFunction, ListReverseFunction, ListSliceFunction, ListSortFunction, LtrimFunction,
    NormalizeFunction, NullIfFunction, RangeFunction, ReplaceFunction, ReverseFunction,
    RightFunction, RtrimFunction, StartsWithFunction, SubstringFunction, TailFunction,
    ToStringFunction, ValueTypeFunction,
};
use functions_temporal::{
    CosineSimilarityFunction, CurrentDateFunction, CurrentTimeFunction, DateAddFunction,
    DateConstructorFunction, DateSubFunction, DurationBetweenFunction, EuclideanDistanceFunction,
    ExtractFunction, LocalDatetimeFunction, LocalTimeFunction, NowFunction, TextMatchFunction,
    TimeConstructorFunction, TimestampToStringFunction, ZonedTimeConstructorFunction,
};

/// Function signature: argument types and return type for plan-time validation.
pub struct FunctionSignature {
    pub arg_types: Vec<GqlType>,
    pub return_type: GqlType,
    pub variadic: bool,
}

/// A scalar function callable from GQL expressions.
pub trait ScalarFunction: Send + Sync {
    /// Function name (used for registry lookup).
    fn name(&self) -> &'static str;
    /// Execute the function with evaluated arguments and evaluation context.
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError>;
    /// Brief description for error messages and MCP tool documentation.
    fn description(&self) -> &'static str {
        ""
    }
    /// Type signature for plan-time validation. None = accept any args.
    fn signature(&self) -> Option<FunctionSignature> {
        None
    }
}

/// Registry of scalar functions.
pub struct FunctionRegistry {
    functions: HashMap<IStr, Arc<dyn ScalarFunction>>,
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    /// Return a reference to the lazily-initialized static builtin registry.
    /// Avoids rebuilding the 101-function HashMap on every query.
    pub fn builtins() -> &'static Self {
        use std::sync::OnceLock;
        static INSTANCE: OnceLock<FunctionRegistry> = OnceLock::new();
        INSTANCE.get_or_init(FunctionRegistry::with_builtins)
    }

    /// Create a registry with all built-in functions.
    pub fn with_builtins() -> Self {
        let mut reg = Self::new();
        reg.register(Arc::new(CoalesceFunction));
        reg.register(Arc::new(CharLengthFunction));
        reg.register(Arc::new(UpperFunction));
        reg.register(Arc::new(LowerFunction));
        reg.register(Arc::new(TrimFunction));
        reg.register(Arc::new(SizeFunction));
        reg.register(Arc::new(DurationFunction));
        reg.register(Arc::new(ZonedDatetimeFunction));
        // Graph element functions (need EvalContext.graph)
        reg.register(Arc::new(IdFunction));
        reg.register(Arc::new(ElementIdFunction));
        reg.register(Arc::new(TypeFunction));
        reg.register(Arc::new(StartNodeFunction));
        reg.register(Arc::new(EndNodeFunction));
        // Degree functions
        reg.register(Arc::new(DegreeFunction));
        reg.register(Arc::new(InDegreeFunction));
        reg.register(Arc::new(OutDegreeFunction));
        // Path functions
        reg.register(Arc::new(PathLengthFunction));
        reg.register(Arc::new(NodesFunction));
        reg.register(Arc::new(EdgesFunction));
        reg.register(Arc::new(IsAcyclicFunction));
        reg.register(Arc::new(IsTrailFunction));
        reg.register(Arc::new(IsSimpleFunction));
        // Property introspection
        reg.register(Arc::new(PropertyNamesFunction));
        // Math functions
        reg.register(Arc::new(AbsFunction));
        reg.register(Arc::new(CeilFunction));
        reg.register(Arc::new(FloorFunction));
        reg.register(Arc::new(RoundFunction));
        reg.register(Arc::new(SqrtFunction));
        reg.register(Arc::new(SignFunction));
        // String functions
        reg.register(Arc::new(ReplaceFunction));
        reg.register(Arc::new(ReverseFunction));
        reg.register(Arc::new(SubstringFunction));
        reg.register(Arc::new(ToStringFunction));
        // Type checking
        reg.register(Arc::new(ValueTypeFunction));
        // Collection functions
        reg.register(Arc::new(HeadFunction));
        reg.register(Arc::new(TailFunction));
        reg.register(Arc::new(LastFunction));
        reg.register(Arc::new(RangeFunction));
        reg.register(Arc::new(KeysFunction));
        // Extended math functions
        reg.register(Arc::new(PowerFn));
        reg.register(Arc::new(LogFunction));
        reg.register(Arc::new(Log10Function));
        reg.register(Arc::new(ExpFunction));
        reg.register(Arc::new(SinFunction));
        reg.register(Arc::new(CosFunction));
        reg.register(Arc::new(TanFunction));
        reg.register(Arc::new(PiFunction));
        reg.register(Arc::new(ModFunction));
        // String/null
        reg.register(Arc::new(NullIfFunction));
        reg.register(Arc::new(LeftFunction));
        reg.register(Arc::new(RightFunction));
        reg.register(Arc::new(LtrimFunction));
        reg.register(Arc::new(RtrimFunction));
        reg.register(Arc::new(StartsWithFunction));
        reg.register(Arc::new(EndsWithFunction));
        reg.register(Arc::new(ContainsFn));
        // Temporal
        reg.register(Arc::new(NowFunction));
        reg.register(Arc::new(CurrentDateFunction));
        reg.register(Arc::new(CurrentTimeFunction));
        reg.register(Arc::new(ExtractFunction));
        reg.register(Arc::new(DateAddFunction));
        reg.register(Arc::new(DateSubFunction));
        reg.register(Arc::new(TimestampToStringFunction));
        // List
        reg.register(Arc::new(ListContainsFunction));
        reg.register(Arc::new(ListSliceFunction));
        reg.register(Arc::new(ListAppendFunction));
        reg.register(Arc::new(ListPrependFunction));
        reg.register(Arc::new(ListLengthFunction));
        reg.register(Arc::new(ListReverseFunction));
        reg.register(Arc::new(ListSortFunction));
        // Graph introspection
        reg.register(Arc::new(PropertiesFunction));
        // Aliases
        reg.register(Arc::new(LengthFunction));
        reg.register(Arc::new(DoubleFunction));

        // Spec-aligned aliases (register same impl under spec name)
        let char_length_fn = reg.get(&IStr::new("char_length")).cloned();
        if let Some(f) = char_length_fn {
            reg.functions.insert(IStr::new("character_length"), f);
        }
        let now_fn = reg.get(&IStr::new("now")).cloned();
        if let Some(f) = now_fn {
            reg.functions.insert(IStr::new("current_timestamp"), f);
        }
        let zdt_fn = reg.get(&IStr::new("zoned_datetime")).cloned();
        if let Some(f) = zdt_fn {
            reg.functions.insert(IStr::new("datetime"), f);
        }
        let nullif_fn = reg.get(&IStr::new("nullif")).cloned();
        if let Some(f) = nullif_fn {
            reg.functions.insert(IStr::new("null_if"), f);
        }

        // Additional math functions (GF02-GF03)
        reg.register(Arc::new(LnFunction));
        reg.register(Arc::new(CotFunction));
        reg.register(Arc::new(SinhFunction));
        reg.register(Arc::new(CoshFunction));
        reg.register(Arc::new(TanhFunction));
        reg.register(Arc::new(AsinFunction));
        reg.register(Arc::new(AcosFunction));
        reg.register(Arc::new(AtanFunction));
        reg.register(Arc::new(Atan2Function));
        reg.register(Arc::new(DegreesFunction));
        reg.register(Arc::new(RadiansFunction));
        reg.register(Arc::new(CardinalityFunction));

        // Temporal constructors (spec SS20.27)
        reg.register(Arc::new(LocalTimeFunction));
        reg.register(Arc::new(LocalDatetimeFunction));
        // local_timestamp = alias for local_datetime
        let ldt_fn = reg.get(&IStr::new("local_datetime")).cloned();
        if let Some(f) = ldt_fn {
            reg.functions.insert(IStr::new("local_timestamp"), f);
        }
        reg.register(Arc::new(DateConstructorFunction));
        reg.register(Arc::new(TimeConstructorFunction));
        reg.register(Arc::new(ZonedTimeConstructorFunction));

        // NORMALIZE function (spec SS20.24)
        reg.register(Arc::new(NormalizeFunction));

        // DURATION_BETWEEN (spec SS20.28)
        reg.register(Arc::new(DurationBetweenFunction));

        // Vector similarity functions
        reg.register(Arc::new(CosineSimilarityFunction));
        reg.register(Arc::new(EuclideanDistanceFunction));

        // Text matching
        reg.register(Arc::new(TextMatchFunction));

        // Embedding function (feature-gated)
        reg.register(Arc::new(crate::runtime::embed::EmbedFunction));

        reg
    }

    pub fn register(&mut self, func: Arc<dyn ScalarFunction>) {
        self.functions.insert(IStr::new(func.name()), func);
    }

    pub fn get(&self, name: &IStr) -> Option<&Arc<dyn ScalarFunction>> {
        self.functions.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::{GqlList, GqlType};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;
    use std::sync::Arc;

    fn test_ctx() -> (SeleneGraph, FunctionRegistry) {
        (SeleneGraph::new(), FunctionRegistry::with_builtins())
    }

    #[test]
    fn registry_with_builtins() {
        let reg = FunctionRegistry::with_builtins();
        assert!(reg.get(&IStr::new("coalesce")).is_some());
        assert!(reg.get(&IStr::new("upper")).is_some());
        assert!(reg.get(&IStr::new("size")).is_some());
        assert!(reg.get(&IStr::new("nonexistent")).is_none());
    }

    #[test]
    fn coalesce_returns_first_non_null() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let func = reg.get(&IStr::new("coalesce")).unwrap();
        let result = func
            .invoke(&[GqlValue::Null, GqlValue::Int(42)], &ctx)
            .unwrap();
        assert_eq!(result, GqlValue::Int(42));
    }

    #[test]
    fn upper_function() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let func = reg.get(&IStr::new("upper")).unwrap();
        let result = func
            .invoke(&[GqlValue::String(SmolStr::new("hello"))], &ctx)
            .unwrap();
        assert_eq!(result, GqlValue::String(SmolStr::new("HELLO")));
    }

    #[test]
    fn size_function_list() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let func = reg.get(&IStr::new("size")).unwrap();
        let list = GqlValue::List(GqlList {
            element_type: GqlType::Int,
            elements: Arc::from(vec![GqlValue::Int(1), GqlValue::Int(2)]),
        });
        let result = func.invoke(&[list], &ctx).unwrap();
        assert_eq!(result, GqlValue::Int(2));
    }

    #[test]
    fn custom_function_registration() {
        struct DoubleFunction;
        impl ScalarFunction for DoubleFunction {
            fn name(&self) -> &'static str {
                "double"
            }
            fn invoke(
                &self,
                args: &[GqlValue],
                _ctx: &EvalContext<'_>,
            ) -> Result<GqlValue, GqlError> {
                match args.first() {
                    Some(GqlValue::Int(i)) => Ok(GqlValue::Int(i * 2)),
                    _ => Err(GqlError::type_error("double requires an int")),
                }
            }
        }

        let g = SeleneGraph::new();
        let mut reg = FunctionRegistry::with_builtins();
        reg.register(Arc::new(DoubleFunction));
        let ctx = EvalContext::new(&g, &reg);
        let func = reg.get(&IStr::new("double")).unwrap();
        let result = func.invoke(&[GqlValue::Int(21)], &ctx).unwrap();
        assert_eq!(result, GqlValue::Int(42));
    }

    // ── ISO 8601 datetime parsing ──

    #[test]
    fn parse_iso8601_utc() {
        match parse_iso8601("2024-01-15T10:30:00Z").unwrap() {
            GqlValue::ZonedDateTime(zdt) => {
                assert_eq!(zdt.offset_seconds, 0);
                // 2024-01-15 10:30:00 UTC
                assert!(zdt.nanos > 0);
            }
            _ => panic!("expected ZonedDateTime"),
        }
    }

    #[test]
    fn parse_iso8601_positive_offset() {
        match parse_iso8601("2024-08-15T14:30:00+02:00").unwrap() {
            GqlValue::ZonedDateTime(zdt) => {
                assert_eq!(zdt.offset_seconds, 7200); // +2 hours
            }
            _ => panic!("expected ZonedDateTime"),
        }
    }

    #[test]
    fn parse_iso8601_negative_offset() {
        match parse_iso8601("2024-12-31T23:59:59-05:00").unwrap() {
            GqlValue::ZonedDateTime(zdt) => {
                assert_eq!(zdt.offset_seconds, -18000); // -5 hours
            }
            _ => panic!("expected ZonedDateTime"),
        }
    }

    #[test]
    fn parse_iso8601_fractional_seconds() {
        match parse_iso8601("2024-08-15T12:30:00.123Z").unwrap() {
            GqlValue::ZonedDateTime(zdt) => {
                assert_eq!(zdt.offset_seconds, 0);
                // Fractional part: 123ms = 123_000_000 nanos
                assert_eq!(zdt.nanos % 1_000_000_000, 123_000_000);
            }
            _ => panic!("expected ZonedDateTime"),
        }
    }

    #[test]
    fn parse_iso8601_same_instant_different_offsets() {
        // 2024-01-15T12:00:00Z and 2024-01-15T14:00:00+02:00 are the same instant
        let GqlValue::ZonedDateTime(utc) = parse_iso8601("2024-01-15T12:00:00Z").unwrap() else {
            panic!("expected ZonedDateTime");
        };
        let GqlValue::ZonedDateTime(plus2) = parse_iso8601("2024-01-15T14:00:00+02:00").unwrap()
        else {
            panic!("expected ZonedDateTime");
        };
        // Both should have the same nanos (UTC)
        assert_eq!(utc.nanos, plus2.nanos);
    }

    #[test]
    fn parse_iso8601_invalid() {
        assert!(parse_iso8601("not-a-date").is_err());
        assert!(parse_iso8601("2024-01-15").is_err()); // missing time
    }

    // ── Vector function tests ──

    #[test]
    fn cosine_similarity_identical() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let f = reg.get(&IStr::new("cosine_similarity")).unwrap();
        let v = GqlValue::Vector(Arc::from(vec![1.0f32, 0.0, 0.0]));
        let result = f.invoke(&[v.clone(), v], &ctx).unwrap();
        match result {
            GqlValue::Float(sim) => assert!((sim - 1.0).abs() < 1e-6),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn cosine_similarity_orthogonal() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let f = reg.get(&IStr::new("cosine_similarity")).unwrap();
        let a = GqlValue::Vector(Arc::from(vec![1.0f32, 0.0, 0.0]));
        let b = GqlValue::Vector(Arc::from(vec![0.0f32, 1.0, 0.0]));
        let result = f.invoke(&[a, b], &ctx).unwrap();
        match result {
            GqlValue::Float(sim) => assert!(sim.abs() < 1e-6),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn cosine_similarity_opposite() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let f = reg.get(&IStr::new("cosine_similarity")).unwrap();
        let a = GqlValue::Vector(Arc::from(vec![1.0f32, 0.0]));
        let b = GqlValue::Vector(Arc::from(vec![-1.0f32, 0.0]));
        let result = f.invoke(&[a, b], &ctx).unwrap();
        match result {
            GqlValue::Float(sim) => assert!((sim + 1.0).abs() < 1e-6),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn cosine_similarity_dimension_mismatch() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let f = reg.get(&IStr::new("cosine_similarity")).unwrap();
        let a = GqlValue::Vector(Arc::from(vec![1.0f32, 0.0]));
        let b = GqlValue::Vector(Arc::from(vec![1.0f32, 0.0, 0.0]));
        assert!(f.invoke(&[a, b], &ctx).is_err());
    }

    #[test]
    fn cosine_similarity_zero_vector() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let f = reg.get(&IStr::new("cosine_similarity")).unwrap();
        let a = GqlValue::Vector(Arc::from(vec![0.0f32, 0.0]));
        let b = GqlValue::Vector(Arc::from(vec![1.0f32, 0.0]));
        let result = f.invoke(&[a, b], &ctx).unwrap();
        match result {
            GqlValue::Float(sim) => assert!(sim.abs() < 1e-6),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn euclidean_distance_known() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let f = reg.get(&IStr::new("euclidean_distance")).unwrap();
        let a = GqlValue::Vector(Arc::from(vec![0.0f32, 0.0]));
        let b = GqlValue::Vector(Arc::from(vec![3.0f32, 4.0]));
        let result = f.invoke(&[a, b], &ctx).unwrap();
        match result {
            GqlValue::Float(d) => assert!((d - 5.0).abs() < 1e-6),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn euclidean_distance_same_point() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let f = reg.get(&IStr::new("euclidean_distance")).unwrap();
        let v = GqlValue::Vector(Arc::from(vec![1.0f32, 2.0, 3.0]));
        let result = f.invoke(&[v.clone(), v], &ctx).unwrap();
        match result {
            GqlValue::Float(d) => assert!(d.abs() < 1e-6),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn cosine_similarity_accepts_list_of_floats() {
        let (g, reg) = test_ctx();
        let ctx = EvalContext::new(&g, &reg);
        let f = reg.get(&IStr::new("cosine_similarity")).unwrap();
        let vec_val = GqlValue::Vector(Arc::from(vec![1.0f32, 0.0]));
        let list_val = GqlValue::List(crate::types::value::GqlList {
            element_type: crate::types::value::GqlType::Float,
            elements: Arc::from(vec![GqlValue::Float(1.0), GqlValue::Float(0.0)]),
        });
        let result = f.invoke(&[vec_val, list_val], &ctx).unwrap();
        match result {
            GqlValue::Float(sim) => assert!((sim - 1.0).abs() < 1e-6),
            _ => panic!("expected Float"),
        }
    }
}
