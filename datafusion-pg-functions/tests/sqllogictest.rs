//! Sqllogictest harness for `datafusion-pg-functions`.
//!
//! Each `.slt` file under `tests/sqllogictest/` is run against a fresh
//! [`SessionContext`] that has had [`register_all`](datafusion_pg_functions::register_all)
//! applied. The harness implements [`sqllogictest::AsyncDB`] by delegating SQL
//! execution to DataFusion and converting the resulting arrow `RecordBatch`es
//! into the row-of-strings format sqllogictest compares against.
//!
//! Add new test cases to `.slt` files; no Rust changes needed unless a new
//! column type shows up that the engine doesn't know how to render.
//!
//! Run with:
//!   cargo test -p datafusion-pg-functions --test sqllogictest
//!
//! Or filter to a single category:
//!   cargo test -p datafusion-pg-functions --test sqllogictest -- math

use std::path::PathBuf;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::ArrayFormatter;
use datafusion::common::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion_pg_functions::register_all;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};

/// Thin wrapper around a `SessionContext` registered with every UDF this
/// crate ships under the categories enabled by Cargo features.
struct PgFunctionsDb {
    ctx: SessionContext,
}

impl PgFunctionsDb {
    fn new() -> Self {
        let mut ctx = SessionContext::new();
        let n = register_all(&mut ctx);
        eprintln!("[sqllogictest] registered {n} UDFs");
        Self { ctx }
    }
}

#[async_trait]
impl AsyncDB for PgFunctionsDb {
    type Error = DataFusionError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>> {
        let df = self.ctx.sql(sql).await?;
        let schema = df.schema().clone();
        let batches = df.collect().await?;

        // DDL/DML: no result columns -> treat as statement completion.
        if schema.fields().is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }

        let types = schema
            .fields()
            .iter()
            .map(|f| arrow_to_slt_type(f.data_type()))
            .collect();

        let mut rows: Vec<Vec<String>> = Vec::new();
        for batch in batches {
            for row in row_strings(&batch)? {
                rows.push(row);
            }
        }

        Ok(DBOutput::Rows { types, rows })
    }

    async fn shutdown(&mut self) {}
}

/// Map an arrow `DataType` to the closest sqllogictest column type.
/// Anything we don't model (lists, structs, binary, ...) falls back to
/// `Any` (`?`), which disables type checking on that column.
fn arrow_to_slt_type(dt: &DataType) -> DefaultColumnType {
    use DefaultColumnType::*;
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Integer,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => FloatingPoint,
        DataType::Boolean => Text, // sqllogictest renders booleans as t/f strings
        _ => Any,
    }
}

/// Render a `RecordBatch` as a `Vec<Vec<String>>` (one inner vec per row).
/// NULL cells become `"NULL"`. Floats use Rust's `{:?}` debug format so that
/// integer-valued floats keep their trailing `.0` (e.g. `1.0`, not `1`).
fn row_strings(batch: &RecordBatch) -> Result<Vec<Vec<String>>> {
    use datafusion::arrow::datatypes::*;

    let formatters: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &Default::default()))
        .collect::<std::result::Result<_, _>>()?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let cells: Vec<String> = (0..batch.num_columns())
            .map(|col| {
                let arr = batch.column(col);
                if arr.is_null(row) {
                    return "NULL".to_string();
                }
                let raw = formatters[col].value(row).to_string();
                // Float debug formatting gives us trailing .0 on integer values,
                // which matches the .slt expected-value convention.
                match arr.data_type() {
                    DataType::Float32 => {
                        // value() already came from the primitive; re-format from the
                        // array directly to ensure {:?} semantics.
                        let v = arr.as_primitive::<Float32Type>().value(row);
                        format!("{v:?}")
                    }
                    DataType::Float64 => {
                        let v = arr.as_primitive::<Float64Type>().value(row);
                        format!("{v:?}")
                    }
                    _ => raw,
                }
            })
            .collect();
        out.push(cells);
    }
    Ok(out)
}

fn slt_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sqllogictest")
}

#[tokio::test]
async fn run_slt_files() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .is_test(true)
        .try_init();

    let dir = slt_dir();
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", dir.display()))
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("slt"))
        .collect();
    entries.sort();
    assert!(!entries.is_empty(), "no .slt files found in {}", dir.display());

    // Optional filter via env var SLT_FILTER=substring to run only matching files.
    let selected: Vec<PathBuf> = match std::env::var("SLT_FILTER") {
        Ok(stem) => entries
            .iter()
            .filter(|p| {
                p.to_string_lossy().contains(&stem)
            })
            .cloned()
            .collect(),
        Err(_) => entries,
    };
    assert!(!selected.is_empty(), "SLT_FILTER matched no files",);

    let mut failures: Vec<String> = Vec::new();
    for path in selected {
        eprintln!("[sqllogictest] running {}", path.display());
        let mut runner = sqllogictest::Runner::new(|| async { Ok(PgFunctionsDb::new()) });
        if let Err(e) = runner.run_file_async(&path).await {
            failures.push(format!("{}:\n{e}", path.display()));
        }
    }
    if !failures.is_empty() {
        panic!("slt failures:\n\n{}\n", failures.join("\n\n"));
    }
}
