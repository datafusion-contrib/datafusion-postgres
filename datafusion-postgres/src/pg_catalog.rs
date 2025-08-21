use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    as_boolean_array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Int16Array, Int32Array,
    RecordBatch, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{CatalogProviderList, SchemaProvider};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{create_udf, SessionContext};
use postgres_types::Oid;
use tokio::sync::RwLock;

const PG_CATALOG_TABLE_PG_AGGREGATE: &str = "pg_aggregate";
const PG_CATALOG_TABLE_PG_AM: &str = "pg_am";
const PG_CATALOG_TABLE_PG_AMOP: &str = "pg_amop";
const PG_CATALOG_TABLE_PG_AMPROC: &str = "pg_amproc";
const PG_CATALOG_TABLE_PG_CAST: &str = "pg_cast";
const PG_CATALOG_TABLE_PG_COLLATION: &str = "pg_collation";
const PG_CATALOG_TABLE_PG_CONVERSION: &str = "pg_conversion";
const PG_CATALOG_TABLE_PG_LANGUAGE: &str = "pg_language";
const PG_CATALOG_TABLE_PG_OPCLASS: &str = "pg_opclass";
const PG_CATALOG_TABLE_PG_OPERATOR: &str = "pg_operator";
const PG_CATALOG_TABLE_PG_OPFAMILY: &str = "pg_opfamily";
const PG_CATALOG_TABLE_PG_PROC: &str = "pg_proc";
const PG_CATALOG_TABLE_PG_RANGE: &str = "pg_range";
const PG_CATALOG_TABLE_PG_TS_CONFIG: &str = "pg_ts_config";
const PG_CATALOG_TABLE_PG_TS_DICT: &str = "pg_ts_dict";
const PG_CATALOG_TABLE_PG_TS_PARSER: &str = "pg_ts_parser";
const PG_CATALOG_TABLE_PG_TS_TEMPLATE: &str = "pg_ts_template";
const PG_CATALOG_TABLE_PG_TYPE: &str = "pg_type";
const PG_CATALOG_TABLE_PG_ATTRIBUTE: &str = "pg_attribute";
const PG_CATALOG_TABLE_PG_ATTRDEF: &str = "pg_attrdef";
const PG_CATALOG_TABLE_PG_AUTH_MEMBERS: &str = "pg_auth_members";
const PG_CATALOG_TABLE_PG_AUTHID: &str = "pg_authid";
const PG_CATALOG_TABLE_PG_CLASS: &str = "pg_class";
const PG_CATALOG_TABLE_PG_CONSTRAINT: &str = "pg_constraint";
const PG_CATALOG_TABLE_PG_DATABASE: &str = "pg_database";
const PG_CATALOG_TABLE_PG_DB_ROLE_SETTING: &str = "pg_db_role_setting";
const PG_CATALOG_TABLE_PG_DEFAULT_ACL: &str = "pg_default_acl";
const PG_CATALOG_TABLE_PG_DEPEND: &str = "pg_depend";
const PG_CATALOG_TABLE_PG_DESCRIPTION: &str = "pg_description";
const PG_CATALOG_TABLE_PG_ENUM: &str = "pg_enum";
const PG_CATALOG_TABLE_PG_EVENT_TRIGGER: &str = "pg_event_trigger";
const PG_CATALOG_TABLE_PG_EXTENSION: &str = "pg_extension";
const PG_CATALOG_TABLE_PG_FOREIGN_DATA_WRAPPER: &str = "pg_foreign_data_wrapper";
const PG_CATALOG_TABLE_PG_FOREIGN_SERVER: &str = "pg_foreign_server";
const PG_CATALOG_TABLE_PG_FOREIGN_TABLE: &str = "pg_foreign_table";
const PG_CATALOG_TABLE_PG_INDEX: &str = "pg_index";
const PG_CATALOG_TABLE_PG_INHERITS: &str = "pg_inherits";
const PG_CATALOG_TABLE_PG_INIT_PRIVS: &str = "pg_init_privs";
const PG_CATALOG_TABLE_PG_LARGEOBJECT: &str = "pg_largeobject";
const PG_CATALOG_TABLE_PG_LARGEOBJECT_METADATA: &str = "pg_largeobject_metadata";
const PG_CATALOG_TABLE_PG_NAMESPACE: &str = "pg_namespace";
const PG_CATALOG_TABLE_PG_PARTITIONED_TABLE: &str = "pg_partitioned_table";
const PG_CATALOG_TABLE_PG_POLICY: &str = "pg_policy";
const PG_CATALOG_TABLE_PG_PUBLICATION: &str = "pg_publication";
const PG_CATALOG_TABLE_PG_PUBLICATION_NAMESPACE: &str = "pg_publication_namespace";
const PG_CATALOG_TABLE_PG_PUBLICATION_REL: &str = "pg_publication_rel";
const PG_CATALOG_TABLE_PG_REPLICATION_ORIGIN: &str = "pg_replication_origin";
const PG_CATALOG_TABLE_PG_REWRITE: &str = "pg_rewrite";
const PG_CATALOG_TABLE_PG_SECLABEL: &str = "pg_seclabel";
const PG_CATALOG_TABLE_PG_SEQUENCE: &str = "pg_sequence";
const PG_CATALOG_TABLE_PG_SHDEPEND: &str = "pg_shdepend";
const PG_CATALOG_TABLE_PG_SHDESCRIPTION: &str = "pg_shdescription";
const PG_CATALOG_TABLE_PG_SHSECLABEL: &str = "pg_shseclabel";
const PG_CATALOG_TABLE_PG_STATISTIC: &str = "pg_statistic";
const PG_CATALOG_TABLE_PG_STATISTIC_EXT: &str = "pg_statistic_ext";
const PG_CATALOG_TABLE_PG_STATISTIC_EXT_DATA: &str = "pg_statistic_ext_data";
const PG_CATALOG_TABLE_PG_SUBSCRIPTION: &str = "pg_subscription";
const PG_CATALOG_TABLE_PG_SUBSCRIPTION_REL: &str = "pg_subscription_rel";
const PG_CATALOG_TABLE_PG_TABLESPACE: &str = "pg_tablespace";
const PG_CATALOG_TABLE_PG_TRIGGER: &str = "pg_trigger";
const PG_CATALOG_TABLE_PG_USER_MAPPING: &str = "pg_user_mapping";

/// Determine PostgreSQL table type (relkind) from DataFusion TableProvider
fn get_table_type(table: &Arc<dyn TableProvider>) -> &'static str {
    // Use Any trait to determine the actual table provider type
    if table.as_any().is::<ViewTable>() {
        "v" // view
    } else {
        "r" // All other table types (StreamingTable, MemTable, etc.) are treated as regular tables
    }
}

/// Determine PostgreSQL table type (relkind) with table name context
fn get_table_type_with_name(
    table: &Arc<dyn TableProvider>,
    table_name: &str,
    schema_name: &str,
) -> &'static str {
    // Check if this is a system catalog table
    if schema_name == "pg_catalog" || schema_name == "information_schema" {
        if table_name.starts_with("pg_")
            || table_name.contains("_table")
            || table_name.contains("_column")
        {
            "r" // System tables are still regular tables in PostgreSQL
        } else {
            "v" // Some system objects might be views
        }
    } else {
        get_table_type(table)
    }
}

pub const PG_CATALOG_TABLES: &[&str] = &[
    PG_CATALOG_TABLE_PG_AGGREGATE,
    PG_CATALOG_TABLE_PG_AM,
    PG_CATALOG_TABLE_PG_AMOP,
    PG_CATALOG_TABLE_PG_AMPROC,
    PG_CATALOG_TABLE_PG_CAST,
    PG_CATALOG_TABLE_PG_COLLATION,
    PG_CATALOG_TABLE_PG_CONVERSION,
    PG_CATALOG_TABLE_PG_LANGUAGE,
    PG_CATALOG_TABLE_PG_OPCLASS,
    PG_CATALOG_TABLE_PG_OPERATOR,
    PG_CATALOG_TABLE_PG_OPFAMILY,
    PG_CATALOG_TABLE_PG_PROC,
    PG_CATALOG_TABLE_PG_RANGE,
    PG_CATALOG_TABLE_PG_TS_CONFIG,
    PG_CATALOG_TABLE_PG_TS_DICT,
    PG_CATALOG_TABLE_PG_TS_PARSER,
    PG_CATALOG_TABLE_PG_TS_TEMPLATE,
    PG_CATALOG_TABLE_PG_TYPE,
    PG_CATALOG_TABLE_PG_ATTRIBUTE,
    PG_CATALOG_TABLE_PG_ATTRDEF,
    PG_CATALOG_TABLE_PG_AUTH_MEMBERS,
    PG_CATALOG_TABLE_PG_AUTHID,
    PG_CATALOG_TABLE_PG_CLASS,
    PG_CATALOG_TABLE_PG_CONSTRAINT,
    PG_CATALOG_TABLE_PG_DATABASE,
    PG_CATALOG_TABLE_PG_DB_ROLE_SETTING,
    PG_CATALOG_TABLE_PG_DEFAULT_ACL,
    PG_CATALOG_TABLE_PG_DEPEND,
    PG_CATALOG_TABLE_PG_DESCRIPTION,
    PG_CATALOG_TABLE_PG_ENUM,
    PG_CATALOG_TABLE_PG_EVENT_TRIGGER,
    PG_CATALOG_TABLE_PG_EXTENSION,
    PG_CATALOG_TABLE_PG_FOREIGN_DATA_WRAPPER,
    PG_CATALOG_TABLE_PG_FOREIGN_SERVER,
    PG_CATALOG_TABLE_PG_FOREIGN_TABLE,
    PG_CATALOG_TABLE_PG_INDEX,
    PG_CATALOG_TABLE_PG_INHERITS,
    PG_CATALOG_TABLE_PG_INIT_PRIVS,
    PG_CATALOG_TABLE_PG_LARGEOBJECT,
    PG_CATALOG_TABLE_PG_LARGEOBJECT_METADATA,
    PG_CATALOG_TABLE_PG_NAMESPACE,
    PG_CATALOG_TABLE_PG_PARTITIONED_TABLE,
    PG_CATALOG_TABLE_PG_POLICY,
    PG_CATALOG_TABLE_PG_PUBLICATION,
    PG_CATALOG_TABLE_PG_PUBLICATION_NAMESPACE,
    PG_CATALOG_TABLE_PG_PUBLICATION_REL,
    PG_CATALOG_TABLE_PG_REPLICATION_ORIGIN,
    PG_CATALOG_TABLE_PG_REWRITE,
    PG_CATALOG_TABLE_PG_SECLABEL,
    PG_CATALOG_TABLE_PG_SEQUENCE,
    PG_CATALOG_TABLE_PG_SHDEPEND,
    PG_CATALOG_TABLE_PG_SHDESCRIPTION,
    PG_CATALOG_TABLE_PG_SHSECLABEL,
    PG_CATALOG_TABLE_PG_STATISTIC,
    PG_CATALOG_TABLE_PG_STATISTIC_EXT,
    PG_CATALOG_TABLE_PG_STATISTIC_EXT_DATA,
    PG_CATALOG_TABLE_PG_SUBSCRIPTION,
    PG_CATALOG_TABLE_PG_SUBSCRIPTION_REL,
    PG_CATALOG_TABLE_PG_TABLESPACE,
    PG_CATALOG_TABLE_PG_TRIGGER,
    PG_CATALOG_TABLE_PG_USER_MAPPING,
];

#[derive(Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
enum OidCacheKey {
    Catalog(String),
    Schema(String, String),
    /// Table by schema and table name
    Table(String, String, String),
}

// Create custom schema provider for pg_catalog
#[derive(Debug)]
pub struct PgCatalogSchemaProvider {
    catalog_list: Arc<dyn CatalogProviderList>,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
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
            PG_CATALOG_TABLE_PG_AGGREGATE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_aggregate.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_AM => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_am.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_AMOP => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_amop.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_AMPROC => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_amproc.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_CAST => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_cast.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_COLLATION => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_collation.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_CONVERSION => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_conversion.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_LANGUAGE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_language.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_OPCLASS => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_opclass.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_OPERATOR => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_operator.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_OPFAMILY => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_opfamily.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_PROC => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_proc.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_RANGE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_range.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_TS_CONFIG => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_ts_config.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_TS_DICT => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_ts_dict.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_TS_PARSER => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_ts_parser.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_TS_TEMPLATE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_ts_template.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_TYPE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_type.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_ATTRIBUTE => {
                let table = Arc::new(PgAttributeTable::new(self.catalog_list.clone()));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_ATTRDEF => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_attrdef.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_AUTH_MEMBERS => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_auth_members.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_AUTHID => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_authid.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_CLASS => {
                let table = Arc::new(PgClassTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_CONSTRAINT => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_constraint.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_DATABASE => {
                let table = Arc::new(PgDatabaseTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_DB_ROLE_SETTING => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_db_role_setting.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_DEFAULT_ACL => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_default_acl.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_DEPEND => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_depend.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_DESCRIPTION => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_description.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_ENUM => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_enum.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_EVENT_TRIGGER => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_event_trigger.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_EXTENSION => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_extension.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_FOREIGN_DATA_WRAPPER => self
                .create_arrow_table(
                    include_bytes!(
                        "../../pg_catalog_arrow_exports/pg_foreign_data_wrapper.feather"
                    )
                    .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_FOREIGN_SERVER => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_foreign_server.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_FOREIGN_TABLE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_foreign_table.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_INDEX => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_index.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_INHERITS => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_inherits.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_INIT_PRIVS => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_init_privs.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_LARGEOBJECT => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_largeobject.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_LARGEOBJECT_METADATA => self
                .create_arrow_table(
                    include_bytes!(
                        "../../pg_catalog_arrow_exports/pg_largeobject_metadata.feather"
                    )
                    .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_NAMESPACE => {
                let table = Arc::new(PgNamespaceTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(Arc::new(
                    StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
                )))
            }
            PG_CATALOG_TABLE_PG_PARTITIONED_TABLE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_partitioned_table.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_POLICY => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_policy.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_PUBLICATION => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_publication.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_PUBLICATION_NAMESPACE => self
                .create_arrow_table(
                    include_bytes!(
                        "../../pg_catalog_arrow_exports/pg_publication_namespace.feather"
                    )
                    .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_PUBLICATION_REL => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_publication_rel.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_REPLICATION_ORIGIN => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_replication_origin.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_REWRITE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_rewrite.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_SECLABEL => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_seclabel.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_SEQUENCE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_sequence.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_SHDEPEND => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_shdepend.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_SHDESCRIPTION => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_shdescription.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_SHSECLABEL => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_shseclabel.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_STATISTIC => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_statistic.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_STATISTIC_EXT => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_statistic_ext.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_STATISTIC_EXT_DATA => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_statistic_ext_data.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_SUBSCRIPTION => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_subscription.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_SUBSCRIPTION_REL => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_subscription_rel.feather")
                        .to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_TABLESPACE => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_tablespace.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_TRIGGER => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_trigger.feather").to_vec(),
                )
                .map(Some),
            PG_CATALOG_TABLE_PG_USER_MAPPING => self
                .create_arrow_table(
                    include_bytes!("../../pg_catalog_arrow_exports/pg_user_mapping.feather")
                        .to_vec(),
                )
                .map(Some),

            _ => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        PG_CATALOG_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}

impl PgCatalogSchemaProvider {
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> PgCatalogSchemaProvider {
        Self {
            catalog_list,
            oid_counter: Arc::new(AtomicU32::new(16384)),
            oid_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create table from dumped arrow data
    fn create_arrow_table(&self, data_bytes: Vec<u8>) -> Result<Arc<dyn TableProvider>> {
        let table = ArrowTable::from_ipc_data(data_bytes)?;
        let streaming_table = StreamingTable::try_new(table.schema.clone(), vec![Arc::new(table)])?;
        Ok(Arc::new(streaming_table))
    }
}

#[derive(Debug, Clone)]
struct PgClassTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
}

impl PgClassTable {
    fn new(
        catalog_list: Arc<dyn CatalogProviderList>,
        oid_counter: Arc<AtomicU32>,
        oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
    ) -> PgClassTable {
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
            oid_counter,
            oid_cache,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(this: PgClassTable) -> Result<RecordBatch> {
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

        let mut oid_cache = this.oid_cache.write().await;
        // Every time when call pg_catalog we generate a new cache and drop the
        // original one in case that schemas or tables were dropped.
        let mut swap_cache = HashMap::new();

        // Iterate through all catalogs and schemas
        for catalog_name in this.catalog_list.catalog_names() {
            let cache_key = OidCacheKey::Catalog(catalog_name.clone());
            let catalog_oid = if let Some(oid) = oid_cache.get(&cache_key) {
                *oid
            } else {
                this.oid_counter.fetch_add(1, Ordering::Relaxed)
            };
            swap_cache.insert(cache_key, catalog_oid);

            if let Some(catalog) = this.catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        let cache_key =
                            OidCacheKey::Schema(catalog_name.clone(), schema_name.clone());
                        let schema_oid = if let Some(oid) = oid_cache.get(&cache_key) {
                            *oid
                        } else {
                            this.oid_counter.fetch_add(1, Ordering::Relaxed)
                        };
                        swap_cache.insert(cache_key, schema_oid);

                        // Add an entry for the schema itself (as a namespace)
                        // (In a full implementation, this would go in pg_namespace)

                        // Now process all tables in this schema
                        for table_name in schema.table_names() {
                            let cache_key = OidCacheKey::Table(
                                catalog_name.clone(),
                                schema_name.clone(),
                                table_name.clone(),
                            );
                            let table_oid = if let Some(oid) = oid_cache.get(&cache_key) {
                                *oid
                            } else {
                                this.oid_counter.fetch_add(1, Ordering::Relaxed)
                            };
                            swap_cache.insert(cache_key, table_oid);

                            if let Some(table) = schema.table(&table_name).await? {
                                // Determine the correct table type based on the table provider and context
                                let table_type =
                                    get_table_type_with_name(&table, &table_name, &schema_name);

                                // Get column count from schema
                                let column_count = table.schema().fields().len() as i16;

                                // Add table entry
                                oids.push(table_oid as i32);
                                relnames.push(table_name.clone());
                                relnamespaces.push(schema_oid as i32);
                                reltypes.push(0); // Simplified: we're not tracking data types
                                reloftypes.push(None);
                                relowners.push(0); // Simplified: no owner tracking
                                relams.push(0); // Default access method
                                relfilenodes.push(table_oid as i32); // Use OID as filenode
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

        *oid_cache = swap_cache;

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
        let batch = RecordBatch::try_new(this.schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl PartitionStream for PgClassTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { PgClassTable::get_data(this).await }),
        ))
    }
}

#[derive(Debug, Clone)]
struct PgNamespaceTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
}

impl PgNamespaceTable {
    pub fn new(
        catalog_list: Arc<dyn CatalogProviderList>,
        oid_counter: Arc<AtomicU32>,
        oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
    ) -> Self {
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
            oid_counter,
            oid_cache,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(this: PgNamespaceTable) -> Result<RecordBatch> {
        // Vectors to store column data
        let mut oids = Vec::new();
        let mut nspnames = Vec::new();
        let mut nspowners = Vec::new();
        let mut nspacls: Vec<Option<String>> = Vec::new();
        let mut options: Vec<Option<String>> = Vec::new();

        // to store all schema-oid mapping temporarily before adding to global oid cache
        let mut schema_oid_cache = HashMap::new();

        let mut oid_cache = this.oid_cache.write().await;

        // Now add all schemas from DataFusion catalogs
        for catalog_name in this.catalog_list.catalog_names() {
            if let Some(catalog) = this.catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    let cache_key = OidCacheKey::Schema(catalog_name.clone(), schema_name.clone());
                    let schema_oid = if let Some(oid) = oid_cache.get(&cache_key) {
                        *oid
                    } else {
                        this.oid_counter.fetch_add(1, Ordering::Relaxed)
                    };
                    schema_oid_cache.insert(cache_key, schema_oid);

                    oids.push(schema_oid as i32);
                    nspnames.push(schema_name.clone());
                    nspowners.push(10); // Default owner
                    nspacls.push(None);
                    options.push(None);
                }
            }
        }

        // remove all schema cache and table of the schema which is no longer exists
        oid_cache.retain(|key, _| match key {
            OidCacheKey::Catalog(..) => true,
            OidCacheKey::Schema(..) => false,
            OidCacheKey::Table(catalog, schema_name, _) => schema_oid_cache
                .contains_key(&OidCacheKey::Schema(catalog.clone(), schema_name.clone())),
        });
        // add new schema cache
        oid_cache.extend(schema_oid_cache);

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(nspnames)),
            Arc::new(Int32Array::from(nspowners)),
            Arc::new(StringArray::from_iter(nspacls.into_iter())),
            Arc::new(StringArray::from_iter(options.into_iter())),
        ];

        // Create a full record batch
        let batch = RecordBatch::try_new(this.schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl PartitionStream for PgNamespaceTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { Self::get_data(this).await }),
        ))
    }
}

#[derive(Debug, Clone)]
struct PgDatabaseTable {
    schema: SchemaRef,
    catalog_list: Arc<dyn CatalogProviderList>,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
}

impl PgDatabaseTable {
    pub fn new(
        catalog_list: Arc<dyn CatalogProviderList>,
        oid_counter: Arc<AtomicU32>,
        oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
    ) -> Self {
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
            oid_counter,
            oid_cache,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(this: PgDatabaseTable) -> Result<RecordBatch> {
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

        // to store all schema-oid mapping temporarily before adding to global oid cache
        let mut catalog_oid_cache = HashMap::new();

        let mut oid_cache = this.oid_cache.write().await;

        // Add a record for each catalog (treating catalogs as "databases")
        for catalog_name in this.catalog_list.catalog_names() {
            let cache_key = OidCacheKey::Catalog(catalog_name.clone());
            let catalog_oid = if let Some(oid) = oid_cache.get(&cache_key) {
                *oid
            } else {
                this.oid_counter.fetch_add(1, Ordering::Relaxed)
            };
            catalog_oid_cache.insert(cache_key, catalog_oid);

            oids.push(catalog_oid as i32);
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
        let default_datname = "postgres".to_string();
        if !datnames.contains(&default_datname) {
            let cache_key = OidCacheKey::Catalog(default_datname.clone());
            let catalog_oid = if let Some(oid) = oid_cache.get(&cache_key) {
                *oid
            } else {
                this.oid_counter.fetch_add(1, Ordering::Relaxed)
            };
            catalog_oid_cache.insert(cache_key, catalog_oid);

            oids.push(catalog_oid as i32);
            datnames.push(default_datname);
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
        let full_batch = RecordBatch::try_new(this.schema.clone(), arrays)?;

        // update cache
        // remove all schema cache and table of the schema which is no longer exists
        oid_cache.retain(|key, _| match key {
            OidCacheKey::Catalog(..) => false,
            OidCacheKey::Schema(catalog, ..) => {
                catalog_oid_cache.contains_key(&OidCacheKey::Catalog(catalog.clone()))
            }
            OidCacheKey::Table(catalog, ..) => {
                catalog_oid_cache.contains_key(&OidCacheKey::Catalog(catalog.clone()))
            }
        });
        // add new schema cache
        oid_cache.extend(catalog_oid_cache);

        Ok(full_batch)
    }
}

impl PartitionStream for PgDatabaseTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { Self::get_data(this).await }),
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

/// A table that reads data from Avro bytes
#[derive(Debug, Clone)]
struct ArrowTable {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
}

impl ArrowTable {
    /// Create a new ArrowTable from bytes
    pub fn from_ipc_data(data: Vec<u8>) -> Result<Self> {
        let cursor = std::io::Cursor::new(data);
        let reader = FileReader::try_new(cursor, None)?;

        let schema = reader.schema();
        let mut batches = Vec::new();

        // Read all record batches from the IPC stream
        for batch in reader {
            batches.push(batch?);
        }

        Ok(Self {
            schema,
            data: batches,
        })
    }
}

impl PartitionStream for ArrowTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let data = self.data.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(data.into_iter().map(Ok)),
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
        // TODO: improve version string generation
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
        let input = &args[0]; // User OID, but we'll ignore for now

        // Create a UTF8 array with default user name
        let mut builder = StringBuilder::new();
        for _ in 0..input.len() {
            builder.append_value("postgres");
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "pg_catalog.pg_get_userbyid",
        vec![DataType::Int32],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_table_is_visible() -> ScalarUDF {
    // Define the function implementation
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let input = &args[0]; // Table OID

        // Always return true
        let mut builder = BooleanBuilder::new();
        for _ in 0..input.len() {
            builder.append_value(true);
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "pg_catalog.pg_table_is_visible",
        vec![DataType::Int32],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_has_table_privilege_3param_udf() -> ScalarUDF {
    // Define the function implementation for 3-parameter version
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let user = &args[0]; // User (can be name or OID)
        let _table = &args[1]; // Table (can be name or OID)
        let _privilege = &args[2]; // Privilege type (SELECT, INSERT, etc.)

        // For now, always return true (full access)
        let mut builder = BooleanArray::builder(user.len());
        for _ in 0..user.len() {
            builder.append_value(true);
        }

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

pub fn create_has_table_privilege_2param_udf() -> ScalarUDF {
    // Define the function implementation for 2-parameter version (current user, table, privilege)
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let table = &args[0]; // Table (can be name or OID)
        let _privilege = &args[1]; // Privilege type (SELECT, INSERT, etc.)

        // For now, always return true (full access for current user)
        let mut builder = BooleanArray::builder(table.len());
        for _ in 0..table.len() {
            builder.append_value(true);
        }
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "has_table_privilege",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_format_type_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let type_oids = &args[0]; // Table (can be name or OID)
        let _type_mods = &args[1]; // Privilege type (SELECT, INSERT, etc.)

        // For now, always return true (full access for current user)
        let mut builder = StringBuilder::new();
        for _ in 0..type_oids.len() {
            builder.append_value("???");
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "format_type",
        vec![DataType::Int32, DataType::Int32],
        DataType::Utf8,
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
    session_context.register_udf(create_has_table_privilege_2param_udf());
    session_context.register_udf(create_pg_table_is_visible());
    session_context.register_udf(create_format_type_udf());

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_load_arrow_data() {
        let table = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_aggregate.feather").to_vec(),
        )
        .expect("Failed to load ipc data");

        assert_eq!(table.schema.fields.len(), 22);
        assert_eq!(table.data.len(), 1);

        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_aggregate.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_am.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_amop.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_amproc.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_cast.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_collation.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_conversion.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_language.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_opclass.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_operator.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_opfamily.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_proc.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_range.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_ts_config.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_ts_dict.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_ts_parser.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_ts_template.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_type.feather").to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_attrdef.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_auth_members.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_authid.feather").to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_constraint.feather").to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_db_role_setting.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_default_acl.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_depend.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_description.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_enum.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_event_trigger.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_extension.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_foreign_data_wrapper.feather")
                .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_foreign_server.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_foreign_table.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_index.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_inherits.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_init_privs.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_largeobject.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_largeobject_metadata.feather")
                .to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_partitioned_table.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_policy.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_publication.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_publication_namespace.feather")
                .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_publication_rel.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_replication_origin.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_rewrite.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_seclabel.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_sequence.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_shdepend.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_shdescription.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_shseclabel.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_statistic.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_statistic_ext.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_statistic_ext_data.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_subscription.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_subscription_rel.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_tablespace.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_trigger.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!("../../pg_catalog_arrow_exports/pg_user_mapping.feather").to_vec(),
        )
        .expect("Failed to load ipc data");
    }
}
