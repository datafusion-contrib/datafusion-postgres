//! Enumerate every scalar / aggregate / window / table function registered
//! on a default `SessionContext`, one name per line on stdout. Used to keep
//! `gen/datafusion_native.txt` honest.
//!
//! Run with: `cargo run -p datafusion-pg-functions --example enumerate_udfs`.

use std::collections::BTreeSet;

use datafusion::prelude::SessionContext;

fn main() {
    let ctx = SessionContext::new();
    let state = ctx.state();

    let mut scalars = BTreeSet::new();
    let mut aggregates = BTreeSet::new();
    let mut windows = BTreeSet::new();
    let mut table_functions = BTreeSet::new();

    for name in state.scalar_functions().keys() {
        scalars.insert(name.to_lowercase());
    }
    for name in state.aggregate_functions().keys() {
        aggregates.insert(name.to_lowercase());
    }
    for name in state.window_functions().keys() {
        windows.insert(name.to_lowercase());
    }
    for name in state.table_functions().keys() {
        table_functions.insert(name.to_lowercase());
    }

    eprintln!(
        "scalar={}, aggregate={}, window={}, table_function={} (total distinct = {})",
        scalars.len(),
        aggregates.len(),
        windows.len(),
        table_functions.len(),
        scalars
            .iter()
            .chain(aggregates.iter())
            .chain(windows.iter())
            .chain(table_functions.iter())
            .collect::<BTreeSet<_>>()
            .len(),
    );

    println!("# scalar UDFs");
    for n in &scalars {
        println!("{n}");
    }
    println!("# aggregate UDFs");
    for n in &aggregates {
        println!("{n}");
    }
    println!("# window UDFs");
    for n in &windows {
        println!("{n}");
    }
    println!("# table functions");
    for n in &table_functions {
        println!("{n}");
    }

    // Names registered under multiple kinds (e.g. first_value is both agg and window).
    let all_kinds: [(&str, &BTreeSet<String>); 3] = [
        ("scalar", &scalars),
        ("aggregate", &aggregates),
        ("window", &windows),
    ];
    for i in 0..all_kinds.len() {
        for j in (i + 1)..all_kinds.len() {
            let (kind_a, set_a) = all_kinds[i];
            let (kind_b, set_b) = all_kinds[j];
            let overlap: BTreeSet<_> = set_a.intersection(set_b).collect();
            if !overlap.is_empty() {
                eprintln!("overlap [{kind_a} ∩ {kind_b}] ({}):", overlap.len());
                for n in &overlap {
                    eprintln!("  {n}");
                }
            }
        }
    }
}
