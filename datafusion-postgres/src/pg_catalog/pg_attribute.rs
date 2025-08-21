use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::CatalogProviderList;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;

#[derive(Debug)]
pub(crate) struct PgAttributeTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgAttributeTable {
    pub(crate) fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
        // Define the schema for pg_attribute
        // This matches PostgreSQL's pg_attribute table columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("attrelid", DataType::Int32, false), // OID of the relation this column belongs to
            Field::new("attname", DataType::Utf8, false),   // Column name
            Field::new("atttypid", DataType::Int32, false), // OID of the column data type
            Field::new("attstattarget", DataType::Int32, false), // Statistics target
            Field::new("attlen", DataType::Int16, false),   // Length of the type
            Field::new("attnum", DataType::Int16, false), // Column number (positive for regular columns)
            Field::new("attndims", DataType::Int32, false), // Number of dimensions for array types
            Field::new("attcacheoff", DataType::Int32, false), // Cache offset
            Field::new("atttypmod", DataType::Int32, false), // Type-specific modifier
            Field::new("attbyval", DataType::Boolean, false), // True if the type is pass-by-value
            Field::new("attalign", DataType::Utf8, false), // Type alignment
            Field::new("attstorage", DataType::Utf8, false), // Storage type
            Field::new("attcompression", DataType::Utf8, true), // Compression method
            Field::new("attnotnull", DataType::Boolean, false), // True if column cannot be null
            Field::new("atthasdef", DataType::Boolean, false), // True if column has a default value
            Field::new("atthasmissing", DataType::Boolean, false), // True if column has missing values
            Field::new("attidentity", DataType::Utf8, false),      // Identity column type
            Field::new("attgenerated", DataType::Utf8, false),     // Generated column type
            Field::new("attisdropped", DataType::Boolean, false), // True if column has been dropped
            Field::new("attislocal", DataType::Boolean, false), // True if column is local to this relation
            Field::new("attinhcount", DataType::Int32, false), // Number of direct inheritance ancestors
            Field::new("attcollation", DataType::Int32, false), // OID of collation
            Field::new("attacl", DataType::Utf8, true),        // Access privileges
            Field::new("attoptions", DataType::Utf8, true),    // Attribute-level options
            Field::new("attfdwoptions", DataType::Utf8, true), // Foreign data wrapper options
            Field::new("attmissingval", DataType::Utf8, true), // Missing value for added columns
        ]));

        Self {
            schema,
            catalog_list,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(
        schema: SchemaRef,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Result<RecordBatch> {
        // Vectors to store column data
        let mut attrelids = Vec::new();
        let mut attnames = Vec::new();
        let mut atttypids = Vec::new();
        let mut attstattargets = Vec::new();
        let mut attlens = Vec::new();
        let mut attnums = Vec::new();
        let mut attndimss = Vec::new();
        let mut attcacheoffs = Vec::new();
        let mut atttymods = Vec::new();
        let mut attbyvals = Vec::new();
        let mut attaligns = Vec::new();
        let mut attstorages = Vec::new();
        let mut attcompressions: Vec<Option<String>> = Vec::new();
        let mut attnotnulls = Vec::new();
        let mut atthasdefs = Vec::new();
        let mut atthasmissings = Vec::new();
        let mut attidentitys = Vec::new();
        let mut attgenerateds = Vec::new();
        let mut attisdroppeds = Vec::new();
        let mut attislocals = Vec::new();
        let mut attinhcounts = Vec::new();
        let mut attcollations = Vec::new();
        let mut attacls: Vec<Option<String>> = Vec::new();
        let mut attoptions: Vec<Option<String>> = Vec::new();
        let mut attfdwoptions: Vec<Option<String>> = Vec::new();
        let mut attmissingvals: Vec<Option<String>> = Vec::new();

        // Start OID counter (should be consistent with pg_class)
        // FIXME: oid
        let mut next_oid = 10000;

        // Iterate through all catalogs and schemas
        for catalog_name in catalog_list.catalog_names() {
            if let Some(catalog) = catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema_provider) = catalog.schema(&schema_name) {
                        // Process all tables in this schema
                        for table_name in schema_provider.table_names() {
                            let table_oid = next_oid;
                            next_oid += 1;

                            if let Some(table) = schema_provider.table(&table_name).await? {
                                let table_schema = table.schema();

                                // Add column entries for this table
                                for (column_idx, field) in table_schema.fields().iter().enumerate()
                                {
                                    let attnum = (column_idx + 1) as i16; // PostgreSQL column numbers start at 1
                                    let (pg_type_oid, type_len, by_val, align, storage) =
                                        Self::datafusion_to_pg_type(field.data_type());

                                    attrelids.push(table_oid);
                                    attnames.push(field.name().clone());
                                    atttypids.push(pg_type_oid);
                                    attstattargets.push(-1); // Default statistics target
                                    attlens.push(type_len);
                                    attnums.push(attnum);
                                    attndimss.push(0); // No array support for now
                                    attcacheoffs.push(-1); // Not cached
                                    atttymods.push(-1); // No type modifiers
                                    attbyvals.push(by_val);
                                    attaligns.push(align.to_string());
                                    attstorages.push(storage.to_string());
                                    attcompressions.push(None); // No compression
                                    attnotnulls.push(!field.is_nullable());
                                    atthasdefs.push(false); // No default values
                                    atthasmissings.push(false); // No missing values
                                    attidentitys.push("".to_string()); // No identity columns
                                    attgenerateds.push("".to_string()); // No generated columns
                                    attisdroppeds.push(false); // Not dropped
                                    attislocals.push(true); // Local to this relation
                                    attinhcounts.push(0); // No inheritance
                                    attcollations.push(0); // Default collation
                                    attacls.push(None); // No ACLs
                                    attoptions.push(None); // No options
                                    attfdwoptions.push(None); // No FDW options
                                    attmissingvals.push(None); // No missing values
                                }
                            }
                        }
                    }
                }
            }
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(attrelids)),
            Arc::new(StringArray::from(attnames)),
            Arc::new(Int32Array::from(atttypids)),
            Arc::new(Int32Array::from(attstattargets)),
            Arc::new(Int16Array::from(attlens)),
            Arc::new(Int16Array::from(attnums)),
            Arc::new(Int32Array::from(attndimss)),
            Arc::new(Int32Array::from(attcacheoffs)),
            Arc::new(Int32Array::from(atttymods)),
            Arc::new(BooleanArray::from(attbyvals)),
            Arc::new(StringArray::from(attaligns)),
            Arc::new(StringArray::from(attstorages)),
            Arc::new(StringArray::from_iter(attcompressions.into_iter())),
            Arc::new(BooleanArray::from(attnotnulls)),
            Arc::new(BooleanArray::from(atthasdefs)),
            Arc::new(BooleanArray::from(atthasmissings)),
            Arc::new(StringArray::from(attidentitys)),
            Arc::new(StringArray::from(attgenerateds)),
            Arc::new(BooleanArray::from(attisdroppeds)),
            Arc::new(BooleanArray::from(attislocals)),
            Arc::new(Int32Array::from(attinhcounts)),
            Arc::new(Int32Array::from(attcollations)),
            Arc::new(StringArray::from_iter(attacls.into_iter())),
            Arc::new(StringArray::from_iter(attoptions.into_iter())),
            Arc::new(StringArray::from_iter(attfdwoptions.into_iter())),
            Arc::new(StringArray::from_iter(attmissingvals.into_iter())),
        ];

        // Create a record batch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        Ok(batch)
    }

    /// Map DataFusion data types to PostgreSQL type information
    fn datafusion_to_pg_type(data_type: &DataType) -> (i32, i16, bool, &'static str, &'static str) {
        match data_type {
            DataType::Boolean => (16, 1, true, "c", "p"),    // bool
            DataType::Int8 => (18, 1, true, "c", "p"),       // char
            DataType::Int16 => (21, 2, true, "s", "p"),      // int2
            DataType::Int32 => (23, 4, true, "i", "p"),      // int4
            DataType::Int64 => (20, 8, true, "d", "p"),      // int8
            DataType::UInt8 => (21, 2, true, "s", "p"),      // Treat as int2
            DataType::UInt16 => (23, 4, true, "i", "p"),     // Treat as int4
            DataType::UInt32 => (20, 8, true, "d", "p"),     // Treat as int8
            DataType::UInt64 => (1700, -1, false, "i", "m"), // Treat as numeric
            DataType::Float32 => (700, 4, true, "i", "p"),   // float4
            DataType::Float64 => (701, 8, true, "d", "p"),   // float8
            DataType::Utf8 => (25, -1, false, "i", "x"),     // text
            DataType::LargeUtf8 => (25, -1, false, "i", "x"), // text
            DataType::Binary => (17, -1, false, "i", "x"),   // bytea
            DataType::LargeBinary => (17, -1, false, "i", "x"), // bytea
            DataType::Date32 => (1082, 4, true, "i", "p"),   // date
            DataType::Date64 => (1082, 4, true, "i", "p"),   // date
            DataType::Time32(_) => (1083, 8, true, "d", "p"), // time
            DataType::Time64(_) => (1083, 8, true, "d", "p"), // time
            DataType::Timestamp(_, _) => (1114, 8, true, "d", "p"), // timestamp
            DataType::Decimal128(_, _) => (1700, -1, false, "i", "m"), // numeric
            DataType::Decimal256(_, _) => (1700, -1, false, "i", "m"), // numeric
            _ => (25, -1, false, "i", "x"),                  // Default to text for unknown types
        }
    }
}

impl PartitionStream for PgAttributeTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let catalog_list = self.catalog_list.clone();
        let schema = Arc::clone(&self.schema);
        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async move { Self::get_data(schema, catalog_list).await }),
        ))
    }
}
