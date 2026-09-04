mod parser;
pub use parser::PostgresCompatibilityParser;
pub mod rules;

#[cfg(feature = "pgvector")]
mod vector_insert;
#[cfg(feature = "pgvector")]
pub use vector_insert::rewrite_vector_insert;
