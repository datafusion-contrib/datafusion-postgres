use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    as_boolean_array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    RecordBatch, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{CatalogProviderList, MemTable, SchemaProvider};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{create_udf, SessionContext};

const PG_CATALOG_TABLE_PG_TYPE: &str = "pg_type";
const PG_CATALOG_TABLE_PG_CLASS: &str = "pg_class";
const PG_CATALOG_TABLE_PG_ATTRIBUTE: &str = "pg_attribute";
const PG_CATALOG_TABLE_PG_NAMESPACE: &str = "pg_namespace";
const PG_CATALOG_TABLE_PG_PROC: &str = "pg_proc";
const PG_CATALOG_TABLE_PG_DATABASE: &str = "pg_database";
const PG_CATALOG_TABLE_PG_AM: &str = "pg_am";

pub const PG_CATALOG_TABLES: &[&str] = &[
    PG_CATALOG_TABLE_PG_TYPE,
    PG_CATALOG_TABLE_PG_CLASS,
    PG_CATALOG_TABLE_PG_ATTRIBUTE,
    PG_CATALOG_TABLE_PG_NAMESPACE,
    PG_CATALOG_TABLE_PG_PROC,
    PG_CATALOG_TABLE_PG_DATABASE,
    PG_CATALOG_TABLE_PG_AM,
];

// Data structure to hold pg_type table data
#[derive(Debug)]
struct PgTypesData {
    oids: Vec<i32>,
    typnames: Vec<String>,
    typnamespaces: Vec<i32>,
    typowners: Vec<i32>,
    typlens: Vec<i16>,
    typbyvals: Vec<bool>,
    typtypes: Vec<String>,
    typcategories: Vec<String>,
    typispreferreds: Vec<bool>,
    typisdefineds: Vec<bool>,
    typdelims: Vec<String>,
    typrelids: Vec<i32>,
    typelems: Vec<i32>,
    typarrays: Vec<i32>,
    typinputs: Vec<String>,
    typoutputs: Vec<String>,
    typreceives: Vec<String>,
    typsends: Vec<String>,
    typmodins: Vec<String>,
    typmodouts: Vec<String>,
    typanalyzes: Vec<String>,
    typaligns: Vec<String>,
    typstorages: Vec<String>,
    typnotnulls: Vec<bool>,
    typbasetypes: Vec<i32>,
    typtymods: Vec<i32>,
    typndimss: Vec<i32>,
    typcollations: Vec<i32>,
    typdefaultbins: Vec<Option<String>>,
    typdefaults: Vec<Option<String>>,
}

impl PgTypesData {
    fn new() -> Self {
        Self {
            oids: Vec::new(),
            typnames: Vec::new(),
            typnamespaces: Vec::new(),
            typowners: Vec::new(),
            typlens: Vec::new(),
            typbyvals: Vec::new(),
            typtypes: Vec::new(),
            typcategories: Vec::new(),
            typispreferreds: Vec::new(),
            typisdefineds: Vec::new(),
            typdelims: Vec::new(),
            typrelids: Vec::new(),
            typelems: Vec::new(),
            typarrays: Vec::new(),
            typinputs: Vec::new(),
            typoutputs: Vec::new(),
            typreceives: Vec::new(),
            typsends: Vec::new(),
            typmodins: Vec::new(),
            typmodouts: Vec::new(),
            typanalyzes: Vec::new(),
            typaligns: Vec::new(),
            typstorages: Vec::new(),
            typnotnulls: Vec::new(),
            typbasetypes: Vec::new(),
            typtymods: Vec::new(),
            typndimss: Vec::new(),
            typcollations: Vec::new(),
            typdefaultbins: Vec::new(),
            typdefaults: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_type(
        &mut self,
        oid: i32,
        typname: &str,
        typnamespace: i32,
        typowner: i32,
        typlen: i16,
        typbyval: bool,
        typtype: &str,
        typcategory: &str,
        typispreferred: bool,
        typisdefined: bool,
        typdelim: &str,
        typrelid: i32,
        typelem: i32,
        typarray: i32,
        typinput: &str,
        typoutput: &str,
        typreceive: &str,
        typsend: &str,
        typmodin: &str,
        typmodout: &str,
        typanalyze: &str,
        typalign: &str,
        typstorage: &str,
        typnotnull: bool,
        typbasetype: i32,
        typtypmod: i32,
        typndims: i32,
        typcollation: i32,
        typdefaultbin: Option<String>,
        typdefault: Option<String>,
    ) {
        self.oids.push(oid);
        self.typnames.push(typname.to_string());
        self.typnamespaces.push(typnamespace);
        self.typowners.push(typowner);
        self.typlens.push(typlen);
        self.typbyvals.push(typbyval);
        self.typtypes.push(typtype.to_string());
        self.typcategories.push(typcategory.to_string());
        self.typispreferreds.push(typispreferred);
        self.typisdefineds.push(typisdefined);
        self.typdelims.push(typdelim.to_string());
        self.typrelids.push(typrelid);
        self.typelems.push(typelem);
        self.typarrays.push(typarray);
        self.typinputs.push(typinput.to_string());
        self.typoutputs.push(typoutput.to_string());
        self.typreceives.push(typreceive.to_string());
        self.typsends.push(typsend.to_string());
        self.typmodins.push(typmodin.to_string());
        self.typmodouts.push(typmodout.to_string());
        self.typanalyzes.push(typanalyze.to_string());
        self.typaligns.push(typalign.to_string());
        self.typstorages.push(typstorage.to_string());
        self.typnotnulls.push(typnotnull);
        self.typbasetypes.push(typbasetype);
        self.typtymods.push(typtypmod);
        self.typndimss.push(typndims);
        self.typcollations.push(typcollation);
        self.typdefaultbins.push(typdefaultbin);
        self.typdefaults.push(typdefault);
    }
}

// Create custom schema provider for pg_catalog
#[derive(Debug)]
pub struct PgCatalogSchemaProvider {
    catalog_list: Arc<dyn CatalogProviderList>,
}

#[async_trait]
impl SchemaProvider for PgCatalogSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        PG_CATALOG_TABLES.iter().map(ToString::to_string).collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        match name.to_ascii_lowercase().as_str() {
            PG_CATALOG_TABLE_PG_TYPE => Ok(Some(self.create_pg_type_table())),
            PG_CATALOG_TABLE_PG_AM => Ok(Some(self.create_pg_am_table())),
            PG_CATALOG_TABLE_PG_CLASS => {
                let table = Arc::new(PgClassTable::new(self.catalog_list.clone()));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_NAMESPACE => {
                let table = Arc::new(PgNamespaceTable::new(self.catalog_list.clone()));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_DATABASE => {
                let table = Arc::new(PgDatabaseTable::new(self.catalog_list.clone()));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_ATTRIBUTE => {
                let table = Arc::new(PgAttributeTable::new(self.catalog_list.clone()));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_PROC => Ok(Some(self.create_pg_proc_table())),
            _ => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        PG_CATALOG_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}

impl PgCatalogSchemaProvider {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> PgCatalogSchemaProvider {
        Self { catalog_list }
    }

    /// Create a populated pg_type table with standard PostgreSQL data types
    fn create_pg_type_table(&self) -> Arc<dyn TableProvider> {
        // Define complete schema for pg_type (matching PostgreSQL)
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", DataType::Int32, false),
            Field::new("typowner", DataType::Int32, false),
            Field::new("typlen", DataType::Int16, false),
            Field::new("typbyval", DataType::Boolean, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typispreferred", DataType::Boolean, false),
            Field::new("typisdefined", DataType::Boolean, false),
            Field::new("typdelim", DataType::Utf8, false),
            Field::new("typrelid", DataType::Int32, false),
            Field::new("typelem", DataType::Int32, false),
            Field::new("typarray", DataType::Int32, false),
            Field::new("typinput", DataType::Utf8, false),
            Field::new("typoutput", DataType::Utf8, false),
            Field::new("typreceive", DataType::Utf8, false),
            Field::new("typsend", DataType::Utf8, false),
            Field::new("typmodin", DataType::Utf8, false),
            Field::new("typmodout", DataType::Utf8, false),
            Field::new("typanalyze", DataType::Utf8, false),
            Field::new("typalign", DataType::Utf8, false),
            Field::new("typstorage", DataType::Utf8, false),
            Field::new("typnotnull", DataType::Boolean, false),
            Field::new("typbasetype", DataType::Int32, false),
            Field::new("typtypmod", DataType::Int32, false),
            Field::new("typndims", DataType::Int32, false),
            Field::new("typcollation", DataType::Int32, false),
            Field::new("typdefaultbin", DataType::Utf8, true),
            Field::new("typdefault", DataType::Utf8, true),
        ]));

        // Create standard PostgreSQL data types
        let pg_types_data = Self::get_standard_pg_types();

        // Create RecordBatch from the data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(pg_types_data.oids)),
            Arc::new(StringArray::from(pg_types_data.typnames)),
            Arc::new(Int32Array::from(pg_types_data.typnamespaces)),
            Arc::new(Int32Array::from(pg_types_data.typowners)),
            Arc::new(Int16Array::from(pg_types_data.typlens)),
            Arc::new(BooleanArray::from(pg_types_data.typbyvals)),
            Arc::new(StringArray::from(pg_types_data.typtypes)),
            Arc::new(StringArray::from(pg_types_data.typcategories)),
            Arc::new(BooleanArray::from(pg_types_data.typispreferreds)),
            Arc::new(BooleanArray::from(pg_types_data.typisdefineds)),
            Arc::new(StringArray::from(pg_types_data.typdelims)),
            Arc::new(Int32Array::from(pg_types_data.typrelids)),
            Arc::new(Int32Array::from(pg_types_data.typelems)),
            Arc::new(Int32Array::from(pg_types_data.typarrays)),
            Arc::new(StringArray::from(pg_types_data.typinputs)),
            Arc::new(StringArray::from(pg_types_data.typoutputs)),
            Arc::new(StringArray::from(pg_types_data.typreceives)),
            Arc::new(StringArray::from(pg_types_data.typsends)),
            Arc::new(StringArray::from(pg_types_data.typmodins)),
            Arc::new(StringArray::from(pg_types_data.typmodouts)),
            Arc::new(StringArray::from(pg_types_data.typanalyzes)),
            Arc::new(StringArray::from(pg_types_data.typaligns)),
            Arc::new(StringArray::from(pg_types_data.typstorages)),
            Arc::new(BooleanArray::from(pg_types_data.typnotnulls)),
            Arc::new(Int32Array::from(pg_types_data.typbasetypes)),
            Arc::new(Int32Array::from(pg_types_data.typtymods)),
            Arc::new(Int32Array::from(pg_types_data.typndimss)),
            Arc::new(Int32Array::from(pg_types_data.typcollations)),
            Arc::new(StringArray::from_iter(
                pg_types_data.typdefaultbins.into_iter(),
            )),
            Arc::new(StringArray::from_iter(
                pg_types_data.typdefaults.into_iter(),
            )),
        ];

        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

        // Create memory table with populated data
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

        Arc::new(provider)
    }

    /// Generate standard PostgreSQL data types for pg_type table
    fn get_standard_pg_types() -> PgTypesData {
        let mut data = PgTypesData::new();

        // Basic data types commonly used
        data.add_type(
            16, "bool", 11, 10, 1, true, "b", "B", true, true, ",", 0, 0, 1000, "boolin",
            "boolout", "boolrecv", "boolsend", "-", "-", "-", "c", "p", false, 0, -1, 0, 0, None,
            None,
        );
        data.add_type(
            17,
            "bytea",
            11,
            10,
            -1,
            false,
            "b",
            "U",
            false,
            true,
            ",",
            0,
            0,
            1001,
            "byteain",
            "byteaout",
            "bytearecv",
            "byteasend",
            "-",
            "-",
            "-",
            "i",
            "x",
            false,
            0,
            -1,
            0,
            0,
            None,
            None,
        );
        data.add_type(
            18, "char", 11, 10, 1, true, "b", "S", false, true, ",", 0, 0, 1002, "charin",
            "charout", "charrecv", "charsend", "-", "-", "-", "c", "p", false, 0, -1, 0, 0, None,
            None,
        );
        data.add_type(
            19, "name", 11, 10, 64, false, "b", "S", false, true, ",", 0, 0, 1003, "namein",
            "nameout", "namerecv", "namesend", "-", "-", "-", "i", "p", false, 0, -1, 0, 0, None,
            None,
        );
        data.add_type(
            20, "int8", 11, 10, 8, true, "b", "N", false, true, ",", 0, 0, 1016, "int8in",
            "int8out", "int8recv", "int8send", "-", "-", "-", "d", "p", false, 0, -1, 0, 0, None,
            None,
        );
        data.add_type(
            21, "int2", 11, 10, 2, true, "b", "N", false, true, ",", 0, 0, 1005, "int2in",
            "int2out", "int2recv", "int2send", "-", "-", "-", "s", "p", false, 0, -1, 0, 0, None,
            None,
        );
        data.add_type(
            23, "int4", 11, 10, 4, true, "b", "N", true, true, ",", 0, 0, 1007, "int4in",
            "int4out", "int4recv", "int4send", "-", "-", "-", "i", "p", false, 0, -1, 0, 0, None,
            None,
        );
        data.add_type(
            25, "text", 11, 10, -1, false, "b", "S", true, true, ",", 0, 0, 1009, "textin",
            "textout", "textrecv", "textsend", "-", "-", "-", "i", "x", false, 0, -1, 0, 100, None,
            None,
        );
        data.add_type(
            700,
            "float4",
            11,
            10,
            4,
            true,
            "b",
            "N",
            false,
            true,
            ",",
            0,
            0,
            1021,
            "float4in",
            "float4out",
            "float4recv",
            "float4send",
            "-",
            "-",
            "-",
            "i",
            "p",
            false,
            0,
            -1,
            0,
            0,
            None,
            None,
        );
        data.add_type(
            701,
            "float8",
            11,
            10,
            8,
            true,
            "b",
            "N",
            true,
            true,
            ",",
            0,
            0,
            1022,
            "float8in",
            "float8out",
            "float8recv",
            "float8send",
            "-",
            "-",
            "-",
            "d",
            "p",
            false,
            0,
            -1,
            0,
            0,
            None,
            None,
        );
        data.add_type(
            1043,
            "varchar",
            11,
            10,
            -1,
            false,
            "b",
            "S",
            false,
            true,
            ",",
            0,
            0,
            1015,
            "varcharin",
            "varcharout",
            "varcharrecv",
            "varcharsend",
            "varchartypmodin",
            "varchartypmodout",
            "-",
            "i",
            "x",
            false,
            0,
            -1,
            0,
            100,
            None,
            None,
        );
        data.add_type(
            1082,
            "date",
            11,
            10,
            4,
            true,
            "b",
            "D",
            false,
            true,
            ",",
            0,
            0,
            1182,
            "date_in",
            "date_out",
            "date_recv",
            "date_send",
            "-",
            "-",
            "-",
            "i",
            "p",
            false,
            0,
            -1,
            0,
            0,
            None,
            None,
        );
        data.add_type(
            1083,
            "time",
            11,
            10,
            8,
            true,
            "b",
            "D",
            false,
            true,
            ",",
            0,
            0,
            1183,
            "time_in",
            "time_out",
            "time_recv",
            "time_send",
            "timetypmodin",
            "timetypmodout",
            "-",
            "d",
            "p",
            false,
            0,
            -1,
            0,
            0,
            None,
            None,
        );
        data.add_type(
            1114,
            "timestamp",
            11,
            10,
            8,
            true,
            "b",
            "D",
            false,
            true,
            ",",
            0,
            0,
            1115,
            "timestamp_in",
            "timestamp_out",
            "timestamp_recv",
            "timestamp_send",
            "timestamptypmodin",
            "timestamptypmodout",
            "-",
            "d",
            "p",
            false,
            0,
            -1,
            0,
            0,
            None,
            None,
        );
        data.add_type(
            1184,
            "timestamptz",
            11,
            10,
            8,
            true,
            "b",
            "D",
            true,
            true,
            ",",
            0,
            0,
            1185,
            "timestamptz_in",
            "timestamptz_out",
            "timestamptz_recv",
            "timestamptz_send",
            "timestamptztypmodin",
            "timestamptztypmodout",
            "-",
            "d",
            "p",
            false,
            0,
            -1,
            0,
            0,
            None,
            None,
        );
        data.add_type(
            1700,
            "numeric",
            11,
            10,
            -1,
            false,
            "b",
            "N",
            false,
            true,
            ",",
            0,
            0,
            1231,
            "numeric_in",
            "numeric_out",
            "numeric_recv",
            "numeric_send",
            "numerictypmodin",
            "numerictypmodout",
            "-",
            "i",
            "m",
            false,
            0,
            -1,
            0,
            0,
            None,
            None,
        );

        data
    }

    /// Create a mock empty table for pg_am
    fn create_pg_am_table(&self) -> Arc<dyn TableProvider> {
        // Define the schema for pg_am
        // This matches PostgreSQL's pg_am table columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("amname", DataType::Utf8, false), // Name of the access method
            Field::new("amhandler", DataType::Int32, false), // OID of handler function
            Field::new("amtype", DataType::Utf8, false), // Type of access method (i=index, t=table)
            Field::new("amstrategies", DataType::Int32, false), // Number of operator strategies
            Field::new("amsupport", DataType::Int32, false), // Number of support routines
            Field::new("amcanorder", DataType::Boolean, false), // Does AM support ordered scans?
            Field::new("amcanorderbyop", DataType::Boolean, false), // Does AM support order by operator result?
            Field::new("amcanbackward", DataType::Boolean, false), // Does AM support backward scanning?
            Field::new("amcanunique", DataType::Boolean, false), // Does AM support unique indexes?
            Field::new("amcanmulticol", DataType::Boolean, false), // Does AM support multi-column indexes?
            Field::new("amoptionalkey", DataType::Boolean, false), // Can first index column be omitted in search?
            Field::new("amsearcharray", DataType::Boolean, false), // Does AM support ScalarArrayOpExpr searches?
            Field::new("amsearchnulls", DataType::Boolean, false), // Does AM support searching for NULL/NOT NULL?
            Field::new("amstorage", DataType::Boolean, false), // Can storage type differ from column type?
            Field::new("amclusterable", DataType::Boolean, false), // Can index be clustered on?
            Field::new("ampredlocks", DataType::Boolean, false), // Does AM manage fine-grained predicate locks?
            Field::new("amcanparallel", DataType::Boolean, false), // Does AM support parallel scan?
            Field::new("amcanbeginscan", DataType::Boolean, false), // Does AM support BRIN index scans?
            Field::new("amcanmarkpos", DataType::Boolean, false), // Does AM support mark/restore positions?
            Field::new("amcanfetch", DataType::Boolean, false), // Does AM support fetching specific tuples?
            Field::new("amkeytype", DataType::Int32, false),    // Type of data in index
        ]));

        // Create memory table with schema
        let provider = MemTable::try_new(schema, vec![]).unwrap();

        Arc::new(provider)
    }

    /// Create a populated pg_proc table with standard PostgreSQL functions
    fn create_pg_proc_table(&self) -> Arc<dyn TableProvider> {
        // Define complete schema for pg_proc (matching PostgreSQL)
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("proname", DataType::Utf8, false), // Function name
            Field::new("pronamespace", DataType::Int32, false), // OID of namespace containing function
            Field::new("proowner", DataType::Int32, false),     // Owner of the function
            Field::new("prolang", DataType::Int32, false),      // Implementation language
            Field::new("procost", DataType::Float32, false),    // Estimated execution cost
            Field::new("prorows", DataType::Float32, false), // Estimated result size for set-returning functions
            Field::new("provariadic", DataType::Int32, false), // Element type of variadic array
            Field::new("prosupport", DataType::Int32, false), // Support function OID
            Field::new("prokind", DataType::Utf8, false), // f=function, p=procedure, a=aggregate, w=window
            Field::new("prosecdef", DataType::Boolean, false), // Security definer flag
            Field::new("proleakproof", DataType::Boolean, false), // Leak-proof flag
            Field::new("proisstrict", DataType::Boolean, false), // Returns null if any argument is null
            Field::new("proretset", DataType::Boolean, false),   // Returns a set (vs scalar)
            Field::new("provolatile", DataType::Utf8, false), // i=immutable, s=stable, v=volatile
            Field::new("proparallel", DataType::Utf8, false), // s=safe, r=restricted, u=unsafe
            Field::new("pronargs", DataType::Int16, false),   // Number of input arguments
            Field::new("pronargdefaults", DataType::Int16, false), // Number of arguments with defaults
            Field::new("prorettype", DataType::Int32, false),      // OID of return type
            Field::new("proargtypes", DataType::Utf8, false),      // Array of argument type OIDs
            Field::new("proallargtypes", DataType::Utf8, true), // Array of all argument type OIDs
            Field::new("proargmodes", DataType::Utf8, true),    // Array of argument modes
            Field::new("proargnames", DataType::Utf8, true),    // Array of argument names
            Field::new("proargdefaults", DataType::Utf8, true), // Expression for argument defaults
            Field::new("protrftypes", DataType::Utf8, true),    // Transform types
            Field::new("prosrc", DataType::Utf8, false),        // Function source code
            Field::new("probin", DataType::Utf8, true),         // Binary file containing function
            Field::new("prosqlbody", DataType::Utf8, true),     // SQL function body
            Field::new("proconfig", DataType::Utf8, true),      // Configuration variables
            Field::new("proacl", DataType::Utf8, true),         // Access privileges
        ]));

        // Create standard PostgreSQL functions
        let pg_proc_data = Self::get_standard_pg_functions();

        // Create RecordBatch from the data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(pg_proc_data.oids)),
            Arc::new(StringArray::from(pg_proc_data.pronames)),
            Arc::new(Int32Array::from(pg_proc_data.pronamespaces)),
            Arc::new(Int32Array::from(pg_proc_data.proowners)),
            Arc::new(Int32Array::from(pg_proc_data.prolangs)),
            Arc::new(Float32Array::from(pg_proc_data.procosts)),
            Arc::new(Float32Array::from(pg_proc_data.prorows)),
            Arc::new(Int32Array::from(pg_proc_data.provariadics)),
            Arc::new(Int32Array::from(pg_proc_data.prosupports)),
            Arc::new(StringArray::from(pg_proc_data.prokinds)),
            Arc::new(BooleanArray::from(pg_proc_data.prosecdefs)),
            Arc::new(BooleanArray::from(pg_proc_data.proleakproofs)),
            Arc::new(BooleanArray::from(pg_proc_data.proisstricts)),
            Arc::new(BooleanArray::from(pg_proc_data.proretsets)),
            Arc::new(StringArray::from(pg_proc_data.provolatiles)),
            Arc::new(StringArray::from(pg_proc_data.proparallels)),
            Arc::new(Int16Array::from(pg_proc_data.pronargs)),
            Arc::new(Int16Array::from(pg_proc_data.pronargdefaults)),
            Arc::new(Int32Array::from(pg_proc_data.prorettypes)),
            Arc::new(StringArray::from(pg_proc_data.proargtypes)),
            Arc::new(StringArray::from_iter(
                pg_proc_data.proallargtypes.into_iter(),
            )),
            Arc::new(StringArray::from_iter(pg_proc_data.proargmodes.into_iter())),
            Arc::new(StringArray::from_iter(pg_proc_data.proargnames.into_iter())),
            Arc::new(StringArray::from_iter(
                pg_proc_data.proargdefaults.into_iter(),
            )),
            Arc::new(StringArray::from_iter(pg_proc_data.protrftypes.into_iter())),
            Arc::new(StringArray::from(pg_proc_data.prosrcs)),
            Arc::new(StringArray::from_iter(pg_proc_data.probins.into_iter())),
            Arc::new(StringArray::from_iter(pg_proc_data.prosqlbodys.into_iter())),
            Arc::new(StringArray::from_iter(pg_proc_data.proconfigs.into_iter())),
            Arc::new(StringArray::from_iter(pg_proc_data.proacls.into_iter())),
        ];

        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

        // Create memory table with populated data
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

        Arc::new(provider)
    }

    /// Generate standard PostgreSQL functions for pg_proc table
    fn get_standard_pg_functions() -> PgProcData {
        let mut data = PgProcData::new();

        // Essential PostgreSQL functions that many tools expect
        data.add_function(
            1242, "boolin", 11, 10, 12, 1.0, 0.0, 0, 0, "f", false, true, true, false, "i", "s", 1,
            0, 16, "2275", None, None, None, None, None, "boolin", None, None, None, None,
        );
        data.add_function(
            1243, "boolout", 11, 10, 12, 1.0, 0.0, 0, 0, "f", false, true, true, false, "i", "s",
            1, 0, 2275, "16", None, None, None, None, None, "boolout", None, None, None, None,
        );
        data.add_function(
            1564, "textin", 11, 10, 12, 1.0, 0.0, 0, 0, "f", false, true, true, false, "i", "s", 1,
            0, 25, "2275", None, None, None, None, None, "textin", None, None, None, None,
        );
        data.add_function(
            1565, "textout", 11, 10, 12, 1.0, 0.0, 0, 0, "f", false, true, true, false, "i", "s",
            1, 0, 2275, "25", None, None, None, None, None, "textout", None, None, None, None,
        );
        data.add_function(
            1242,
            "version",
            11,
            10,
            12,
            1.0,
            0.0,
            0,
            0,
            "f",
            false,
            true,
            false,
            false,
            "s",
            "s",
            0,
            0,
            25,
            "",
            None,
            None,
            None,
            None,
            None,
            "SELECT 'DataFusion PostgreSQL 48.0.0 on x86_64-pc-linux-gnu'",
            None,
            None,
            None,
            None,
        );

        data
    }
}

// Data structure to hold pg_proc table data
#[derive(Debug)]
struct PgProcData {
    oids: Vec<i32>,
    pronames: Vec<String>,
    pronamespaces: Vec<i32>,
    proowners: Vec<i32>,
    prolangs: Vec<i32>,
    procosts: Vec<f32>,
    prorows: Vec<f32>,
    provariadics: Vec<i32>,
    prosupports: Vec<i32>,
    prokinds: Vec<String>,
    prosecdefs: Vec<bool>,
    proleakproofs: Vec<bool>,
    proisstricts: Vec<bool>,
    proretsets: Vec<bool>,
    provolatiles: Vec<String>,
    proparallels: Vec<String>,
    pronargs: Vec<i16>,
    pronargdefaults: Vec<i16>,
    prorettypes: Vec<i32>,
    proargtypes: Vec<String>,
    proallargtypes: Vec<Option<String>>,
    proargmodes: Vec<Option<String>>,
    proargnames: Vec<Option<String>>,
    proargdefaults: Vec<Option<String>>,
    protrftypes: Vec<Option<String>>,
    prosrcs: Vec<String>,
    probins: Vec<Option<String>>,
    prosqlbodys: Vec<Option<String>>,
    proconfigs: Vec<Option<String>>,
    proacls: Vec<Option<String>>,
}

impl PgProcData {
    fn new() -> Self {
        Self {
            oids: Vec::new(),
            pronames: Vec::new(),
            pronamespaces: Vec::new(),
            proowners: Vec::new(),
            prolangs: Vec::new(),
            procosts: Vec::new(),
            prorows: Vec::new(),
            provariadics: Vec::new(),
            prosupports: Vec::new(),
            prokinds: Vec::new(),
            prosecdefs: Vec::new(),
            proleakproofs: Vec::new(),
            proisstricts: Vec::new(),
            proretsets: Vec::new(),
            provolatiles: Vec::new(),
            proparallels: Vec::new(),
            pronargs: Vec::new(),
            pronargdefaults: Vec::new(),
            prorettypes: Vec::new(),
            proargtypes: Vec::new(),
            proallargtypes: Vec::new(),
            proargmodes: Vec::new(),
            proargnames: Vec::new(),
            proargdefaults: Vec::new(),
            protrftypes: Vec::new(),
            prosrcs: Vec::new(),
            probins: Vec::new(),
            prosqlbodys: Vec::new(),
            proconfigs: Vec::new(),
            proacls: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_function(
        &mut self,
        oid: i32,
        proname: &str,
        pronamespace: i32,
        proowner: i32,
        prolang: i32,
        procost: f32,
        prorows: f32,
        provariadic: i32,
        prosupport: i32,
        prokind: &str,
        prosecdef: bool,
        proleakproof: bool,
        proisstrict: bool,
        proretset: bool,
        provolatile: &str,
        proparallel: &str,
        pronargs: i16,
        pronargdefaults: i16,
        prorettype: i32,
        proargtypes: &str,
        proallargtypes: Option<String>,
        proargmodes: Option<String>,
        proargnames: Option<String>,
        proargdefaults: Option<String>,
        protrftypes: Option<String>,
        prosrc: &str,
        probin: Option<String>,
        prosqlbody: Option<String>,
        proconfig: Option<String>,
        proacl: Option<String>,
    ) {
        self.oids.push(oid);
        self.pronames.push(proname.to_string());
        self.pronamespaces.push(pronamespace);
        self.proowners.push(proowner);
        self.prolangs.push(prolang);
        self.procosts.push(procost);
        self.prorows.push(prorows);
        self.provariadics.push(provariadic);
        self.prosupports.push(prosupport);
        self.prokinds.push(prokind.to_string());
        self.prosecdefs.push(prosecdef);
        self.proleakproofs.push(proleakproof);
        self.proisstricts.push(proisstrict);
        self.proretsets.push(proretset);
        self.provolatiles.push(provolatile.to_string());
        self.proparallels.push(proparallel.to_string());
        self.pronargs.push(pronargs);
        self.pronargdefaults.push(pronargdefaults);
        self.prorettypes.push(prorettype);
        self.proargtypes.push(proargtypes.to_string());
        self.proallargtypes.push(proallargtypes);
        self.proargmodes.push(proargmodes);
        self.proargnames.push(proargnames);
        self.proargdefaults.push(proargdefaults);
        self.protrftypes.push(protrftypes);
        self.prosrcs.push(prosrc.to_string());
        self.probins.push(probin);
        self.prosqlbodys.push(prosqlbody);
        self.proconfigs.push(proconfig);
        self.proacls.push(proacl);
    }
}

#[derive(Debug)]
struct PgClassTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgClassTable {
    fn new(catalog_list: Arc<dyn CatalogProviderList>) -> PgClassTable {
        // Define the schema for pg_class
        // This matches key columns from PostgreSQL's pg_class
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("relname", DataType::Utf8, false), // Name of the table, index, view, etc.
            Field::new("relnamespace", DataType::Int32, false), // OID of the namespace that contains this relation
            Field::new("reltype", DataType::Int32, false), // OID of the data type (composite type) this table describes
            Field::new("reloftype", DataType::Int32, true), // OID of the composite type for typed table, 0 otherwise
            Field::new("relowner", DataType::Int32, false), // Owner of the relation
            Field::new("relam", DataType::Int32, false), // If this is an index, the access method used
            Field::new("relfilenode", DataType::Int32, false), // Name of the on-disk file of this relation
            Field::new("reltablespace", DataType::Int32, false), // Tablespace OID for this relation
            Field::new("relpages", DataType::Int32, false), // Size of the on-disk representation in pages
            Field::new("reltuples", DataType::Float64, false), // Number of tuples
            Field::new("relallvisible", DataType::Int32, false), // Number of all-visible pages
            Field::new("reltoastrelid", DataType::Int32, false), // OID of the TOAST table
            Field::new("relhasindex", DataType::Boolean, false), // True if this is a table and it has (or recently had) any indexes
            Field::new("relisshared", DataType::Boolean, false), // True if this table is shared across all databases
            Field::new("relpersistence", DataType::Utf8, false), // p=permanent table, u=unlogged table, t=temporary table
            Field::new("relkind", DataType::Utf8, false), // r=ordinary table, i=index, S=sequence, v=view, etc.
            Field::new("relnatts", DataType::Int16, false), // Number of user columns
            Field::new("relchecks", DataType::Int16, false), // Number of CHECK constraints
            Field::new("relhasrules", DataType::Boolean, false), // True if table has (or once had) rules
            Field::new("relhastriggers", DataType::Boolean, false), // True if table has (or once had) triggers
            Field::new("relhassubclass", DataType::Boolean, false), // True if table or index has (or once had) any inheritance children
            Field::new("relrowsecurity", DataType::Boolean, false), // True if row security is enabled
            Field::new("relforcerowsecurity", DataType::Boolean, false), // True if row security forced for owners
            Field::new("relispopulated", DataType::Boolean, false), // True if relation is populated (not true for some materialized views)
            Field::new("relreplident", DataType::Utf8, false), // Columns used to form "replica identity" for rows
            Field::new("relispartition", DataType::Boolean, false), // True if table is a partition
            Field::new("relrewrite", DataType::Int32, true), // OID of a rule that rewrites this relation
            Field::new("relfrozenxid", DataType::Int32, false), // All transaction IDs before this have been replaced with a permanent ("frozen") transaction ID
            Field::new("relminmxid", DataType::Int32, false), // All Multixact IDs before this have been replaced with a transaction ID
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
        let mut oids = Vec::new();
        let mut relnames = Vec::new();
        let mut relnamespaces = Vec::new();
        let mut reltypes = Vec::new();
        let mut reloftypes = Vec::new();
        let mut relowners = Vec::new();
        let mut relams = Vec::new();
        let mut relfilenodes = Vec::new();
        let mut reltablespaces = Vec::new();
        let mut relpages = Vec::new();
        let mut reltuples = Vec::new();
        let mut relallvisibles = Vec::new();
        let mut reltoastrelids = Vec::new();
        let mut relhasindexes = Vec::new();
        let mut relisshareds = Vec::new();
        let mut relpersistences = Vec::new();
        let mut relkinds = Vec::new();
        let mut relnattses = Vec::new();
        let mut relcheckses = Vec::new();
        let mut relhasruleses = Vec::new();
        let mut relhastriggersses = Vec::new();
        let mut relhassubclasses = Vec::new();
        let mut relrowsecurities = Vec::new();
        let mut relforcerowsecurities = Vec::new();
        let mut relispopulateds = Vec::new();
        let mut relreplidents = Vec::new();
        let mut relispartitions = Vec::new();
        let mut relrewrites = Vec::new();
        let mut relfrozenxids = Vec::new();
        let mut relminmxids = Vec::new();

        // Start OID counter (this is simplistic and would need to be more robust in practice)
        let mut next_oid = 10000;

        // Iterate through all catalogs and schemas
        for catalog_name in catalog_list.catalog_names() {
            if let Some(catalog) = catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        let schema_oid = next_oid;
                        next_oid += 1;

                        // Add an entry for the schema itself (as a namespace)
                        // (In a full implementation, this would go in pg_namespace)

                        // Now process all tables in this schema
                        for table_name in schema.table_names() {
                            let table_oid = next_oid;
                            next_oid += 1;

                            if let Some(table) = schema.table(&table_name).await? {
                                // TODO: correct table type
                                let table_type = "r";

                                // Get column count from schema
                                let column_count = table.schema().fields().len() as i16;

                                // Add table entry
                                oids.push(table_oid);
                                relnames.push(table_name.clone());
                                relnamespaces.push(schema_oid);
                                reltypes.push(0); // Simplified: we're not tracking data types
                                reloftypes.push(None);
                                relowners.push(0); // Simplified: no owner tracking
                                relams.push(0); // Default access method
                                relfilenodes.push(table_oid); // Use OID as filenode
                                reltablespaces.push(0); // Default tablespace
                                relpages.push(1); // Default page count
                                reltuples.push(0.0); // No row count stats
                                relallvisibles.push(0);
                                reltoastrelids.push(0);
                                relhasindexes.push(false);
                                relisshareds.push(false);
                                relpersistences.push("p".to_string()); // Permanent
                                relkinds.push(table_type.to_string());
                                relnattses.push(column_count);
                                relcheckses.push(0);
                                relhasruleses.push(false);
                                relhastriggersses.push(false);
                                relhassubclasses.push(false);
                                relrowsecurities.push(false);
                                relforcerowsecurities.push(false);
                                relispopulateds.push(true);
                                relreplidents.push("d".to_string()); // Default
                                relispartitions.push(false);
                                relrewrites.push(None);
                                relfrozenxids.push(0);
                                relminmxids.push(0);
                            }
                        }
                    }
                }
            }
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(relnames)),
            Arc::new(Int32Array::from(relnamespaces)),
            Arc::new(Int32Array::from(reltypes)),
            Arc::new(Int32Array::from_iter(reloftypes.into_iter())),
            Arc::new(Int32Array::from(relowners)),
            Arc::new(Int32Array::from(relams)),
            Arc::new(Int32Array::from(relfilenodes)),
            Arc::new(Int32Array::from(reltablespaces)),
            Arc::new(Int32Array::from(relpages)),
            Arc::new(Float64Array::from_iter(reltuples.into_iter())),
            Arc::new(Int32Array::from(relallvisibles)),
            Arc::new(Int32Array::from(reltoastrelids)),
            Arc::new(BooleanArray::from(relhasindexes)),
            Arc::new(BooleanArray::from(relisshareds)),
            Arc::new(StringArray::from(relpersistences)),
            Arc::new(StringArray::from(relkinds)),
            Arc::new(Int16Array::from(relnattses)),
            Arc::new(Int16Array::from(relcheckses)),
            Arc::new(BooleanArray::from(relhasruleses)),
            Arc::new(BooleanArray::from(relhastriggersses)),
            Arc::new(BooleanArray::from(relhassubclasses)),
            Arc::new(BooleanArray::from(relrowsecurities)),
            Arc::new(BooleanArray::from(relforcerowsecurities)),
            Arc::new(BooleanArray::from(relispopulateds)),
            Arc::new(StringArray::from(relreplidents)),
            Arc::new(BooleanArray::from(relispartitions)),
            Arc::new(Int32Array::from_iter(relrewrites.into_iter())),
            Arc::new(Int32Array::from(relfrozenxids)),
            Arc::new(Int32Array::from(relminmxids)),
        ];

        // Create a record batch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl PartitionStream for PgClassTable {
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

#[derive(Debug)]
struct PgNamespaceTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgNamespaceTable {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
        // Define the schema for pg_namespace
        // This matches the columns from PostgreSQL's pg_namespace
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("nspname", DataType::Utf8, false), // Name of the namespace (schema)
            Field::new("nspowner", DataType::Int32, false), // Owner of the namespace
            Field::new("nspacl", DataType::Utf8, true), // Access privileges
            Field::new("options", DataType::Utf8, true), // Schema-level options
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
        let mut oids = Vec::new();
        let mut nspnames = Vec::new();
        let mut nspowners = Vec::new();
        let mut nspacls: Vec<Option<String>> = Vec::new();
        let mut options: Vec<Option<String>> = Vec::new();

        // Start OID counter (should be consistent with the values used in pg_class)
        let mut next_oid = 10000;

        // Add standard PostgreSQL system schemas
        // pg_catalog schema (OID 11)
        oids.push(11);
        nspnames.push("pg_catalog".to_string());
        nspowners.push(10); // Default superuser
        nspacls.push(None);
        options.push(None);

        // public schema (OID 2200)
        oids.push(2200);
        nspnames.push("public".to_string());
        nspowners.push(10); // Default superuser
        nspacls.push(None);
        options.push(None);

        // information_schema (OID 12)
        oids.push(12);
        nspnames.push("information_schema".to_string());
        nspowners.push(10); // Default superuser
        nspacls.push(None);
        options.push(None);

        // Now add all schemas from DataFusion catalogs
        for catalog_name in catalog_list.catalog_names() {
            if let Some(catalog) = catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    // Skip schemas we've already added as system schemas
                    if schema_name == "pg_catalog"
                        || schema_name == "public"
                        || schema_name == "information_schema"
                    {
                        continue;
                    }

                    let schema_oid = next_oid;
                    next_oid += 1;

                    oids.push(schema_oid);
                    nspnames.push(schema_name.clone());
                    nspowners.push(10); // Default owner
                    nspacls.push(None);
                    options.push(None);
                }
            }
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(nspnames)),
            Arc::new(Int32Array::from(nspowners)),
            Arc::new(StringArray::from_iter(nspacls.into_iter())),
            Arc::new(StringArray::from_iter(options.into_iter())),
        ];

        // Create a full record batch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl PartitionStream for PgNamespaceTable {
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

#[derive(Debug)]
struct PgDatabaseTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgDatabaseTable {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
        // Define the schema for pg_database
        // This matches PostgreSQL's pg_database table columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("datname", DataType::Utf8, false), // Database name
            Field::new("datdba", DataType::Int32, false), // Database owner's user ID
            Field::new("encoding", DataType::Int32, false), // Character encoding
            Field::new("datcollate", DataType::Utf8, false), // LC_COLLATE for this database
            Field::new("datctype", DataType::Utf8, false), // LC_CTYPE for this database
            Field::new("datistemplate", DataType::Boolean, false), // If true, database can be used as a template
            Field::new("datallowconn", DataType::Boolean, false), // If false, no one can connect to this database
            Field::new("datconnlimit", DataType::Int32, false), // Max number of concurrent connections (-1=no limit)
            Field::new("datlastsysoid", DataType::Int32, false), // Last system OID in database
            Field::new("datfrozenxid", DataType::Int32, false), // Frozen XID for this database
            Field::new("datminmxid", DataType::Int32, false),   // Minimum multixact ID
            Field::new("dattablespace", DataType::Int32, false), // Default tablespace for this database
            Field::new("datacl", DataType::Utf8, true),          // Access privileges
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
        let mut oids = Vec::new();
        let mut datnames = Vec::new();
        let mut datdbas = Vec::new();
        let mut encodings = Vec::new();
        let mut datcollates = Vec::new();
        let mut datctypes = Vec::new();
        let mut datistemplates = Vec::new();
        let mut datallowconns = Vec::new();
        let mut datconnlimits = Vec::new();
        let mut datlastsysoids = Vec::new();
        let mut datfrozenxids = Vec::new();
        let mut datminmxids = Vec::new();
        let mut dattablespaces = Vec::new();
        let mut datacles: Vec<Option<String>> = Vec::new();

        // Start OID counter (this is simplistic and would need to be more robust in practice)
        let mut next_oid = 16384; // Standard PostgreSQL starting OID for user databases

        // Add a record for each catalog (treating catalogs as "databases")
        for catalog_name in catalog_list.catalog_names() {
            let oid = next_oid;
            next_oid += 1;

            oids.push(oid);
            datnames.push(catalog_name.clone());
            datdbas.push(10); // Default owner (assuming 10 = postgres user)
            encodings.push(6); // 6 = UTF8 in PostgreSQL
            datcollates.push("en_US.UTF-8".to_string()); // Default collation
            datctypes.push("en_US.UTF-8".to_string()); // Default ctype
            datistemplates.push(false);
            datallowconns.push(true);
            datconnlimits.push(-1); // No connection limit
            datlastsysoids.push(100000); // Arbitrary last system OID
            datfrozenxids.push(1); // Simplified transaction ID
            datminmxids.push(1); // Simplified multixact ID
            dattablespaces.push(1663); // Default tablespace (1663 = pg_default in PostgreSQL)
            datacles.push(None); // No specific ACLs
        }

        // Always include a "postgres" database entry if not already present
        // (This is for compatibility with tools that expect it)
        if !datnames.contains(&"postgres".to_string()) {
            let oid = next_oid;

            oids.push(oid);
            datnames.push("postgres".to_string());
            datdbas.push(10);
            encodings.push(6);
            datcollates.push("en_US.UTF-8".to_string());
            datctypes.push("en_US.UTF-8".to_string());
            datistemplates.push(false);
            datallowconns.push(true);
            datconnlimits.push(-1);
            datlastsysoids.push(100000);
            datfrozenxids.push(1);
            datminmxids.push(1);
            dattablespaces.push(1663);
            datacles.push(None);
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(datnames)),
            Arc::new(Int32Array::from(datdbas)),
            Arc::new(Int32Array::from(encodings)),
            Arc::new(StringArray::from(datcollates)),
            Arc::new(StringArray::from(datctypes)),
            Arc::new(BooleanArray::from(datistemplates)),
            Arc::new(BooleanArray::from(datallowconns)),
            Arc::new(Int32Array::from(datconnlimits)),
            Arc::new(Int32Array::from(datlastsysoids)),
            Arc::new(Int32Array::from(datfrozenxids)),
            Arc::new(Int32Array::from(datminmxids)),
            Arc::new(Int32Array::from(dattablespaces)),
            Arc::new(StringArray::from_iter(datacles.into_iter())),
        ];

        // Create a full record batch
        let full_batch = RecordBatch::try_new(schema.clone(), arrays)?;
        Ok(full_batch)
    }
}

impl PartitionStream for PgDatabaseTable {
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

#[derive(Debug)]
struct PgAttributeTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl PgAttributeTable {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
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

pub fn create_current_schemas_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let input = as_boolean_array(&args[0]);

        // Create a UTF8 array with a single value
        let mut values = vec!["public"];
        // include implicit schemas
        if input.value(0) {
            values.push("information_schema");
            values.push("pg_catalog");
        }

        let list_array = SingleRowListArrayBuilder::new(Arc::new(StringArray::from(values)));

        let array: ArrayRef = Arc::new(list_array.build_list_array());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_schemas",
        vec![DataType::Boolean],
        DataType::List(Arc::new(Field::new("schema", DataType::Utf8, false))),
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_current_schema_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Create a UTF8 array with a single value
        let mut builder = StringBuilder::new();
        builder.append_value("public");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_schema",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_version_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Create a UTF8 array with version information
        let mut builder = StringBuilder::new();
        builder
            .append_value("DataFusion PostgreSQL 48.0.0 on x86_64-pc-linux-gnu, compiled by Rust");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "version",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_pg_get_userbyid_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let _input = &args[0]; // User OID, but we'll ignore for now

        // Create a UTF8 array with default user name
        let mut builder = StringBuilder::new();
        builder.append_value("postgres");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "pg_get_userbyid",
        vec![DataType::Int32],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_has_table_privilege_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let _user = &args[0]; // User (can be name or OID)
        let _table = &args[1]; // Table (can be name or OID)
        let _privilege = &args[2]; // Privilege type (SELECT, INSERT, etc.)

        // For now, always return true (full access)
        let mut builder = BooleanArray::builder(1);
        builder.append_value(true);
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "has_table_privilege",
        vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(func),
    )
}

/// Install pg_catalog and postgres UDFs to current `SessionContext`
pub fn setup_pg_catalog(
    session_context: &SessionContext,
    catalog_name: &str,
) -> Result<(), Box<DataFusionError>> {
    let pg_catalog = PgCatalogSchemaProvider::new(session_context.state().catalog_list().clone());
    session_context
        .catalog(catalog_name)
        .ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Catalog not found when registering pg_catalog: {catalog_name}"
            ))
        })?
        .register_schema("pg_catalog", Arc::new(pg_catalog))?;

    session_context.register_udf(create_current_schema_udf());
    session_context.register_udf(create_current_schemas_udf());
    session_context.register_udf(create_version_udf());
    session_context.register_udf(create_pg_get_userbyid_udf());
    session_context.register_udf(create_has_table_privilege_udf());

    Ok(())
}
