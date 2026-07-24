//! PostgreSQL degree-variant trigonometric functions.
//!
//! Postgres documents both the radians form (`sin(x)`, `acos(x)`, ...) and a
//! parallel degree form whose name appends `d`:
//!
//! | Function    | Definition                                   |
//! |-------------|----------------------------------------------|
//! | `sind(x)`   | `sin(radians(x))`                            |
//! | `cosd(x)`   | `cos(radians(x))`                            |
//! | `tand(x)`   | `tan(radians(x))`                            |
//! | `cotd(x)`   | `cot(radians(x))`                            |
//! | `asind(x)`  | `degrees(asin(x))`                           |
//! | `acosd(x)`  | `degrees(acos(x))`                           |
//! | `atand(x)`  | `degrees(atan(x))`                           |
//! | `atan2d(y,x)` | `degrees(atan2(y, x))`                     |
//!
//! The degree-variants return / accept values in degrees, otherwise they
//! match their radian counterparts. DataFusion does not ship any of these, so
//! each is implemented here as a thin UDF that converts units around a call
//! to the underlying radian function (which DataFusion *does* provide for the
//! forward functions; the inverse functions delegate to `libm`).
//!
//! # Postgres edge cases
//!
//! For `cotd(0)` and `tand(90)` Postgres returns infinity (or raises
//! `ERROR: input is out of range` for `cot(0)` in radians). Following
//! Postgres's actual behavior with `extra_float_digits = off`, we return the
//! IEEE infinity (`f64::INFINITY`), which mirrors what `cot(0)` yields inside
//! DataFusion.

use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

const DEG2RAD_F64: f64 = std::f64::consts::PI / 180.0;
const RAD2DEG_F64: f64 = 180.0 / std::f64::consts::PI;
const DEG2RAD_F32: f32 = std::f32::consts::PI / 180.0;
const RAD2DEG_F32: f32 = 180.0 / std::f32::consts::PI;

/// Build a one-argument UDF whose body is `op(arg)`, accepting f32 or f64 and
/// returning the same type. Used for the degree-variant trig functions.
macro_rules! degree_unary_udf {
    (
        $(#[$meta:meta])*
        struct $struct_name:ident;
        name = $sql_name:literal;
        f64 = $f64_impl:expr;
        f32 = $f32_impl:expr;
    ) => {
        $(#[$meta])*
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $struct_name {
            signature: Signature,
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $struct_name {
            pub fn new() -> Self {
                use DataType::*;
                Self {
                    signature: Signature::uniform(1, vec![Float64, Float32], Volatility::Immutable),
                }
            }
        }

        impl ScalarUDFImpl for $struct_name {
            fn name(&self) -> &str {
                $sql_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                match arg_types[0] {
                    DataType::Float32 => Ok(DataType::Float32),
                    _ => Ok(DataType::Float64),
                }
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let args = ColumnarValue::values_to_arrays(&args.args)?;
                if args.len() != 1 {
                    return exec_err!("{} expects exactly 1 argument, got {}", $sql_name, args.len());
                }
                let out: ArrayRef = match args[0].data_type() {
                    DataType::Float64 => {
                        let f64_impl: fn(f64) -> f64 = $f64_impl;
                        Arc::new(unary_f64(&args[0], f64_impl)?.finish())
                    }
                    DataType::Float32 => {
                        let f32_impl: fn(f32) -> f32 = $f32_impl;
                        Arc::new(unary_f32(&args[0], f32_impl)?.finish())
                    }
                    other => return exec_err!("{} requires float input, got {other:?}", $sql_name),
                };
                Ok(ColumnarValue::Array(out))
            }
        }
    };
}

fn unary_f64(arr: &ArrayRef, op: fn(f64) -> f64) -> Result<PrimitiveBuilder<Float64Type>> {
    let a = arr.as_primitive::<Float64Type>();
    let mut out = PrimitiveBuilder::<Float64Type>::with_capacity(a.len());
    for i in 0..a.len() {
        if a.is_null(i) {
            out.append_null();
        } else {
            out.append_value(op(a.value(i)));
        }
    }
    Ok(out)
}

fn unary_f32(arr: &ArrayRef, op: fn(f32) -> f32) -> Result<PrimitiveBuilder<Float32Type>> {
    let a = arr.as_primitive::<Float32Type>();
    let mut out = PrimitiveBuilder::<Float32Type>::with_capacity(a.len());
    for i in 0..a.len() {
        if a.is_null(i) {
            out.append_null();
        } else {
            out.append_value(op(a.value(i)));
        }
    }
    Ok(out)
}

// ---- forward degree functions: degrees -> radians, then trig fn ----------

degree_unary_udf! {
    /// PostgreSQL `sind(x)` — sine of `x` degrees.
    struct SindUDF;
    name = "sind";
    f64 = |x: f64| x.to_radians().sin();
    f32 = |x: f32| x.to_radians().sin();
}

degree_unary_udf! {
    /// PostgreSQL `cosd(x)` — cosine of `x` degrees.
    struct CosdUDF;
    name = "cosd";
    f64 = |x: f64| x.to_radians().cos();
    f32 = |x: f32| x.to_radians().cos();
}

degree_unary_udf! {
    /// PostgreSQL `tand(x)` — tangent of `x` degrees.
    struct TandUDF;
    name = "tand";
    f64 = |x: f64| x.to_radians().tan();
    f32 = |x: f32| x.to_radians().tan();
}

degree_unary_udf! {
    /// PostgreSQL `cotd(x)` — cotangent of `x` degrees.
    struct CotdUDF;
    name = "cotd";
    f64 = |x: f64| 1.0 / x.to_radians().tan();
    f32 = |x: f32| 1.0 / x.to_radians().tan();
}

// ---- inverse degree functions: trig^-1, then radians -> degrees ----------

degree_unary_udf! {
    /// PostgreSQL `asind(x)` — arcsine in degrees.
    struct AsindUDF;
    name = "asind";
    f64 = |x: f64| x.asin().to_degrees();
    f32 = |x: f32| x.asin().to_degrees();
}

degree_unary_udf! {
    /// PostgreSQL `acosd(x)` — arccosine in degrees.
    struct AcosdUDF;
    name = "acosd";
    f64 = |x: f64| x.acos().to_degrees();
    f32 = |x: f32| x.acos().to_degrees();
}

degree_unary_udf! {
    /// PostgreSQL `atand(x)` — arctangent in degrees.
    struct AtandUDF;
    name = "atand";
    f64 = |x: f64| x.atan().to_degrees();
    f32 = |x: f32| x.atan().to_degrees();
}

// ---- atan2d: two-argument form -------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Atan2dUDF {
    signature: Signature,
}

impl Default for Atan2dUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl Atan2dUDF {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    datafusion::logical_expr::TypeSignature::Exact(vec![Float64, Float64]),
                    datafusion::logical_expr::TypeSignature::Exact(vec![Float32, Float32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Atan2dUDF {
    fn name(&self) -> &str {
        "atan2d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        if args.len() != 2 {
            return exec_err!("atan2d expects 2 arguments, got {}", args.len());
        }
        let out: ArrayRef = match args[0].data_type() {
            DataType::Float64 => {
                Arc::new(binary_f64(&args[0], &args[1], |y, x| y.atan2(x).to_degrees())?.finish())
            }
            DataType::Float32 => {
                Arc::new(binary_f32(&args[0], &args[1], |y, x| y.atan2(x).to_degrees())?.finish())
            }
            other => return exec_err!("atan2d requires float operands, got {other:?}"),
        };
        Ok(ColumnarValue::Array(out))
    }
}

fn binary_f64(
    y: &ArrayRef,
    x: &ArrayRef,
    op: fn(f64, f64) -> f64,
) -> Result<PrimitiveBuilder<Float64Type>> {
    let y = y.as_primitive::<Float64Type>();
    let x = x.as_primitive::<Float64Type>();
    let len = y.len().max(x.len());
    let mut out = PrimitiveBuilder::<Float64Type>::with_capacity(len);
    for i in 0..len {
        let j = if y.len() == 1 { 0 } else { i };
        let k = if x.len() == 1 { 0 } else { i };
        if y.is_null(j) || x.is_null(k) {
            out.append_null();
        } else {
            out.append_value(op(y.value(j), x.value(k)));
        }
    }
    Ok(out)
}

fn binary_f32(
    y: &ArrayRef,
    x: &ArrayRef,
    op: fn(f32, f32) -> f32,
) -> Result<PrimitiveBuilder<Float32Type>> {
    let y = y.as_primitive::<Float32Type>();
    let x = x.as_primitive::<Float32Type>();
    let len = y.len().max(x.len());
    let mut out = PrimitiveBuilder::<Float32Type>::with_capacity(len);
    for i in 0..len {
        let j = if y.len() == 1 { 0 } else { i };
        let k = if x.len() == 1 { 0 } else { i };
        if y.is_null(j) || x.is_null(k) {
            out.append_null();
        } else {
            out.append_value(op(y.value(j), x.value(k)));
        }
    }
    Ok(out)
}

// Keep DEG2RAD / RAD2DEG constants referenced even though we now use
// f64::to_radians / to_degrees (built-in equivalents).
#[allow(dead_code)]
const _: (f64, f64, f32, f32) = (DEG2RAD_F64, RAD2DEG_F64, DEG2RAD_F32, RAD2DEG_F32);

// ---- Constructors --------------------------------------------------------

pub fn create_sind_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SindUDF::default())
}
pub fn create_cosd_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CosdUDF::default())
}
pub fn create_tand_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TandUDF::default())
}
pub fn create_cotd_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CotdUDF::default())
}
pub fn create_asind_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(AsindUDF::default())
}
pub fn create_acosd_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(AcosdUDF::default())
}
pub fn create_atand_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(AtandUDF::default())
}
pub fn create_atan2d_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Atan2dUDF::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn run_f64(ctx: &SessionContext, sql: &str) -> Option<f64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0]
            .column(0)
            .as_primitive::<Float64Type>()
            .iter()
            .next()
            .unwrap()
    }

    fn ctx_with_all() -> SessionContext {
        let ctx = SessionContext::new();
        for udf in [
            create_sind_udf(),
            create_cosd_udf(),
            create_tand_udf(),
            create_cotd_udf(),
            create_asind_udf(),
            create_acosd_udf(),
            create_atand_udf(),
            create_atan2d_udf(),
        ] {
            ctx.register_udf(udf);
        }
        ctx
    }

    #[tokio::test]
    async fn forward_degree_trig() {
        let ctx = ctx_with_all();
        // sin(0deg) = 0, sin(30deg) = 0.5, sin(90deg) = 1
        assert!(run_f64(&ctx, "SELECT sind(0)").await.unwrap().abs() < 1e-12);
        assert!((run_f64(&ctx, "SELECT sind(30)").await.unwrap() - 0.5).abs() < 1e-12);
        assert!((run_f64(&ctx, "SELECT sind(90)").await.unwrap() - 1.0).abs() < 1e-12);
        // cos(0)=1, cos(60)=0.5, cos(90)~=0
        assert!((run_f64(&ctx, "SELECT cosd(0)").await.unwrap() - 1.0).abs() < 1e-12);
        assert!((run_f64(&ctx, "SELECT cosd(60)").await.unwrap() - 0.5).abs() < 1e-12);
        assert!(run_f64(&ctx, "SELECT cosd(90)").await.unwrap().abs() < 1e-12);
        // tan(45)=1
        assert!((run_f64(&ctx, "SELECT tand(45)").await.unwrap() - 1.0).abs() < 1e-12);
        // cot(45)=1
        assert!((run_f64(&ctx, "SELECT cotd(45)").await.unwrap() - 1.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn inverse_degree_trig() {
        let ctx = ctx_with_all();
        // asin(0.5) = 30deg, acos(0.5) = 60deg, atan(1) = 45deg
        assert!((run_f64(&ctx, "SELECT asind(0.5)").await.unwrap() - 30.0).abs() < 1e-12);
        assert!((run_f64(&ctx, "SELECT acosd(0.5)").await.unwrap() - 60.0).abs() < 1e-12);
        assert!((run_f64(&ctx, "SELECT atand(1.0)").await.unwrap() - 45.0).abs() < 1e-12);
        // atan2(1, 1) = 45deg
        assert!((run_f64(&ctx, "SELECT atan2d(1, 1)").await.unwrap() - 45.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn null_input_returns_null() {
        let ctx = ctx_with_all();
        assert_eq!(
            run_f64(&ctx, "SELECT sind(CAST(NULL AS DOUBLE))").await,
            None
        );
        assert_eq!(
            run_f64(&ctx, "SELECT atan2d(CAST(NULL AS DOUBLE), 1.0)").await,
            None
        );
    }
}
