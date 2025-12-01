use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    as_boolean_array, ArrayRef, AsArray, BooleanBuilder, Int32Builder, RecordBatch, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, SchemaRef};
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{MemTable, SchemaProvider, TableFunctionImpl};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{create_udf, Expr, SessionContext};
use postgres_types::Oid;
use tokio::sync::RwLock;

use crate::pg_catalog::catalog_info::CatalogInfo;
use crate::pg_catalog::context::PgCatalogContextProvider;
use crate::pg_catalog::empty_table::EmptyTable;

pub mod catalog_info;
pub mod context;
pub mod empty_table;
pub mod format_type;
pub mod has_privilege_udf;
pub mod pg_attribute;
pub mod pg_class;
pub mod pg_database;
pub mod pg_get_expr_udf;
pub mod pg_namespace;
pub mod pg_replication_slot;
pub mod pg_roles;
pub mod pg_settings;
pub mod pg_stat_gssapi;
pub mod pg_tables;
pub mod pg_views;

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
const PG_CATALOG_VIEW_PG_SETTINGS: &str = "pg_settings";
const PG_CATALOG_VIEW_PG_VIEWS: &str = "pg_views";
const PG_CATALOG_VIEW_PG_MATVIEWS: &str = "pg_matviews";
const PG_CATALOG_VIEW_PG_ROLES: &str = "pg_roles";
const PG_CATALOG_VIEW_PG_TABLES: &str = "pg_tables";
const PG_CATALOG_VIEW_PG_STAT_GSSAPI: &str = "pg_stat_gssapi";
const PG_CATALOG_VIEW_PG_STAT_USER_TABLES: &str = "pg_stat_user_tables";
const PG_CATALOG_VIEW_PG_REPLICATION_SLOTS: &str = "pg_replication_slots";

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
    PG_CATALOG_VIEW_PG_ROLES,
    PG_CATALOG_VIEW_PG_SETTINGS,
    PG_CATALOG_VIEW_PG_STAT_GSSAPI,
    PG_CATALOG_VIEW_PG_TABLES,
    PG_CATALOG_VIEW_PG_VIEWS,
    PG_CATALOG_VIEW_PG_MATVIEWS,
    PG_CATALOG_VIEW_PG_STAT_USER_TABLES,
    PG_CATALOG_VIEW_PG_REPLICATION_SLOTS,
];

#[derive(Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) enum OidCacheKey {
    Catalog(String),
    Schema(String, String),
    /// Table by schema and table name
    Table(String, String, String),
}

// Create custom schema provider for pg_catalog
#[derive(Debug)]
pub struct PgCatalogSchemaProvider<C, P> {
    catalog_list: C,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
    static_tables: Arc<PgCatalogStaticTables>,
    context_provider: P,
}

#[async_trait]
impl<C: CatalogInfo, P: PgCatalogContextProvider> SchemaProvider for PgCatalogSchemaProvider<C, P> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        PG_CATALOG_TABLES.iter().map(ToString::to_string).collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(table) = self.build_table_by_name(name)? {
            let table_provider = table.try_into_table_provider()?;
            Ok(Some(table_provider))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        PG_CATALOG_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}

impl<C: CatalogInfo, P: PgCatalogContextProvider> PgCatalogSchemaProvider<C, P> {
    pub fn try_new(
        catalog_list: C,
        static_tables: Arc<PgCatalogStaticTables>,
        context_provider: P,
    ) -> Result<PgCatalogSchemaProvider<C, P>> {
        Ok(Self {
            catalog_list,
            oid_counter: Arc::new(AtomicU32::new(16384)),
            oid_cache: Arc::new(RwLock::new(HashMap::new())),
            static_tables,
            context_provider,
        })
    }

    pub fn build_table_by_name(&self, name: &str) -> Result<Option<PgCatalogTable>> {
        match name.to_ascii_lowercase().as_str() {
            PG_CATALOG_TABLE_PG_AGGREGATE => {
                Ok(Some(self.static_tables.pg_aggregate.clone().into()))
            }
            PG_CATALOG_TABLE_PG_AM => Ok(Some(self.static_tables.pg_am.clone().into())),
            PG_CATALOG_TABLE_PG_AMOP => Ok(Some(self.static_tables.pg_amop.clone().into())),
            PG_CATALOG_TABLE_PG_AMPROC => Ok(Some(self.static_tables.pg_amproc.clone().into())),
            PG_CATALOG_TABLE_PG_CAST => Ok(Some(self.static_tables.pg_cast.clone().into())),
            PG_CATALOG_TABLE_PG_COLLATION => {
                Ok(Some(self.static_tables.pg_collation.clone().into()))
            }
            PG_CATALOG_TABLE_PG_CONVERSION => {
                Ok(Some(self.static_tables.pg_conversion.clone().into()))
            }
            PG_CATALOG_TABLE_PG_LANGUAGE => Ok(Some(self.static_tables.pg_language.clone().into())),
            PG_CATALOG_TABLE_PG_OPCLASS => Ok(Some(self.static_tables.pg_opclass.clone().into())),
            PG_CATALOG_TABLE_PG_OPERATOR => Ok(Some(self.static_tables.pg_operator.clone().into())),
            PG_CATALOG_TABLE_PG_OPFAMILY => Ok(Some(self.static_tables.pg_opfamily.clone().into())),
            PG_CATALOG_TABLE_PG_PROC => Ok(Some(self.static_tables.pg_proc.clone().into())),
            PG_CATALOG_TABLE_PG_RANGE => Ok(Some(self.static_tables.pg_range.clone().into())),
            PG_CATALOG_TABLE_PG_TS_CONFIG => {
                Ok(Some(self.static_tables.pg_ts_config.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TS_DICT => Ok(Some(self.static_tables.pg_ts_dict.clone().into())),
            PG_CATALOG_TABLE_PG_TS_PARSER => {
                Ok(Some(self.static_tables.pg_ts_parser.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TS_TEMPLATE => {
                Ok(Some(self.static_tables.pg_ts_template.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TYPE => Ok(Some(self.static_tables.pg_type.clone().into())),
            PG_CATALOG_TABLE_PG_ATTRDEF => Ok(Some(self.static_tables.pg_attrdef.clone().into())),
            PG_CATALOG_TABLE_PG_AUTH_MEMBERS => {
                Ok(Some(self.static_tables.pg_auth_members.clone().into()))
            }
            PG_CATALOG_TABLE_PG_AUTHID => Ok(Some(self.static_tables.pg_authid.clone().into())),

            PG_CATALOG_TABLE_PG_CONSTRAINT => {
                Ok(Some(self.static_tables.pg_constraint.clone().into()))
            }

            PG_CATALOG_TABLE_PG_DB_ROLE_SETTING => {
                Ok(Some(self.static_tables.pg_db_role_setting.clone().into()))
            }
            PG_CATALOG_TABLE_PG_DEFAULT_ACL => {
                Ok(Some(self.static_tables.pg_default_acl.clone().into()))
            }
            PG_CATALOG_TABLE_PG_DEPEND => Ok(Some(self.static_tables.pg_depend.clone().into())),
            PG_CATALOG_TABLE_PG_DESCRIPTION => {
                Ok(Some(self.static_tables.pg_description.clone().into()))
            }
            PG_CATALOG_TABLE_PG_ENUM => Ok(Some(self.static_tables.pg_enum.clone().into())),
            PG_CATALOG_TABLE_PG_EVENT_TRIGGER => {
                Ok(Some(self.static_tables.pg_event_trigger.clone().into()))
            }
            PG_CATALOG_TABLE_PG_EXTENSION => {
                Ok(Some(self.static_tables.pg_extension.clone().into()))
            }
            PG_CATALOG_TABLE_PG_FOREIGN_DATA_WRAPPER => Ok(Some(
                self.static_tables.pg_foreign_data_wrapper.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_FOREIGN_SERVER => {
                Ok(Some(self.static_tables.pg_foreign_server.clone().into()))
            }
            PG_CATALOG_TABLE_PG_FOREIGN_TABLE => {
                Ok(Some(self.static_tables.pg_foreign_table.clone().into()))
            }
            PG_CATALOG_TABLE_PG_INDEX => Ok(Some(self.static_tables.pg_index.clone().into())),
            PG_CATALOG_TABLE_PG_INHERITS => Ok(Some(self.static_tables.pg_inherits.clone().into())),
            PG_CATALOG_TABLE_PG_INIT_PRIVS => {
                Ok(Some(self.static_tables.pg_init_privs.clone().into()))
            }
            PG_CATALOG_TABLE_PG_LARGEOBJECT => {
                Ok(Some(self.static_tables.pg_largeobject.clone().into()))
            }
            PG_CATALOG_TABLE_PG_LARGEOBJECT_METADATA => Ok(Some(
                self.static_tables.pg_largeobject_metadata.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_PARTITIONED_TABLE => {
                Ok(Some(self.static_tables.pg_partitioned_table.clone().into()))
            }
            PG_CATALOG_TABLE_PG_POLICY => Ok(Some(self.static_tables.pg_policy.clone().into())),
            PG_CATALOG_TABLE_PG_PUBLICATION => {
                Ok(Some(self.static_tables.pg_publication.clone().into()))
            }
            PG_CATALOG_TABLE_PG_PUBLICATION_NAMESPACE => Ok(Some(
                self.static_tables.pg_publication_namespace.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_PUBLICATION_REL => {
                Ok(Some(self.static_tables.pg_publication_rel.clone().into()))
            }
            PG_CATALOG_TABLE_PG_REPLICATION_ORIGIN => Ok(Some(
                self.static_tables.pg_replication_origin.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_REWRITE => Ok(Some(self.static_tables.pg_rewrite.clone().into())),
            PG_CATALOG_TABLE_PG_SECLABEL => Ok(Some(self.static_tables.pg_seclabel.clone().into())),
            PG_CATALOG_TABLE_PG_SEQUENCE => Ok(Some(self.static_tables.pg_sequence.clone().into())),
            PG_CATALOG_TABLE_PG_SHDEPEND => Ok(Some(self.static_tables.pg_shdepend.clone().into())),
            PG_CATALOG_TABLE_PG_SHDESCRIPTION => {
                Ok(Some(self.static_tables.pg_shdescription.clone().into()))
            }
            PG_CATALOG_TABLE_PG_SHSECLABEL => {
                Ok(Some(self.static_tables.pg_shseclabel.clone().into()))
            }
            PG_CATALOG_TABLE_PG_STATISTIC => {
                Ok(Some(self.static_tables.pg_statistic.clone().into()))
            }
            PG_CATALOG_TABLE_PG_STATISTIC_EXT => {
                Ok(Some(self.static_tables.pg_statistic_ext.clone().into()))
            }
            PG_CATALOG_TABLE_PG_STATISTIC_EXT_DATA => Ok(Some(
                self.static_tables.pg_statistic_ext_data.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_SUBSCRIPTION => {
                Ok(Some(self.static_tables.pg_subscription.clone().into()))
            }
            PG_CATALOG_TABLE_PG_SUBSCRIPTION_REL => {
                Ok(Some(self.static_tables.pg_subscription_rel.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TABLESPACE => {
                Ok(Some(self.static_tables.pg_tablespace.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TRIGGER => Ok(Some(self.static_tables.pg_trigger.clone().into())),
            PG_CATALOG_TABLE_PG_USER_MAPPING => {
                Ok(Some(self.static_tables.pg_user_mapping.clone().into()))
            }

            PG_CATALOG_TABLE_PG_ATTRIBUTE => {
                let table = Arc::new(pg_attribute::PgAttributeTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_TABLE_PG_CLASS => {
                let table = Arc::new(pg_class::PgClassTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_TABLE_PG_DATABASE => {
                let table = Arc::new(pg_database::PgDatabaseTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_TABLE_PG_NAMESPACE => {
                let table = Arc::new(pg_namespace::PgNamespaceTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_VIEW_PG_TABLES => {
                let table = Arc::new(pg_tables::PgTablesTable::new(self.catalog_list.clone()));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_VIEW_PG_SETTINGS => {
                let table = Arc::new(pg_settings::PgSettingsView::new());
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }

            PG_CATALOG_VIEW_PG_STAT_GSSAPI => {
                let table = Arc::new(pg_stat_gssapi::PgStatGssApiTable::new());
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_VIEW_PG_ROLES => {
                let table = Arc::new(pg_roles::PgRolesTable::new(self.context_provider.clone()));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }

            PG_CATALOG_VIEW_PG_VIEWS => Ok(Some(pg_views::pg_views().into())),
            PG_CATALOG_VIEW_PG_MATVIEWS => Ok(Some(pg_views::pg_matviews().into())),
            PG_CATALOG_VIEW_PG_STAT_USER_TABLES => Ok(Some(pg_views::pg_stat_user_tables().into())),
            PG_CATALOG_VIEW_PG_REPLICATION_SLOTS => {
                Ok(Some(pg_replication_slot::pg_replication_slots().into()))
            }

            _ => Ok(None),
        }
    }
}

/// A table that reads data from Avro bytes
#[derive(Debug, Clone)]
pub struct ArrowTable {
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

    /// Convert the arrow data into datafusion MemTable
    pub fn try_into_memtable(&self) -> Result<Arc<MemTable>> {
        let mem_table = MemTable::try_new(self.schema.clone(), vec![self.data.clone()])?;
        Ok(Arc::new(mem_table))
    }

    pub fn data(&self) -> &[RecordBatch] {
        &self.data
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl TableFunctionImpl for ArrowTable {
    fn call(&self, _args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let table = self.try_into_memtable()?;
        Ok(table)
    }
}

/// an enum to wrap all kinds of tables
pub enum PgCatalogTable {
    Static(Arc<ArrowTable>),
    Dynamic(Arc<dyn PartitionStream>),
    Empty(EmptyTable),
}

impl From<Arc<ArrowTable>> for PgCatalogTable {
    fn from(t: Arc<ArrowTable>) -> PgCatalogTable {
        PgCatalogTable::Static(t)
    }
}

impl From<EmptyTable> for PgCatalogTable {
    fn from(t: EmptyTable) -> PgCatalogTable {
        Self::Empty(t)
    }
}

impl PgCatalogTable {
    pub fn try_into_table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        match self {
            Self::Static(t) => {
                let memtable = t.try_into_memtable()?;
                Ok(memtable)
            }
            Self::Dynamic(t) => {
                let streaming_table =
                    StreamingTable::try_new(Arc::clone(t.schema()), vec![t.clone()])?;
                Ok(Arc::new(streaming_table))
            }
            Self::Empty(t) => {
                let memtable = t.try_into_memtable()?;
                Ok(memtable)
            }
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Static(t) => t.schema(),
            Self::Dynamic(t) => t.schema().clone(),
            Self::Empty(t) => t.schema(),
        }
    }
}

/// pg_catalog table as datafusion table provider
///
/// This implementation only contains static tables
#[derive(Debug, Clone)]
pub struct PgCatalogStaticTables {
    pub pg_aggregate: Arc<ArrowTable>,
    pub pg_am: Arc<ArrowTable>,
    pub pg_amop: Arc<ArrowTable>,
    pub pg_amproc: Arc<ArrowTable>,
    pub pg_cast: Arc<ArrowTable>,
    pub pg_collation: Arc<ArrowTable>,
    pub pg_conversion: Arc<ArrowTable>,
    pub pg_language: Arc<ArrowTable>,
    pub pg_opclass: Arc<ArrowTable>,
    pub pg_operator: Arc<ArrowTable>,
    pub pg_opfamily: Arc<ArrowTable>,
    pub pg_proc: Arc<ArrowTable>,
    pub pg_range: Arc<ArrowTable>,
    pub pg_ts_config: Arc<ArrowTable>,
    pub pg_ts_dict: Arc<ArrowTable>,
    pub pg_ts_parser: Arc<ArrowTable>,
    pub pg_ts_template: Arc<ArrowTable>,
    pub pg_type: Arc<ArrowTable>,
    pub pg_attrdef: Arc<ArrowTable>,
    pub pg_auth_members: Arc<ArrowTable>,
    pub pg_authid: Arc<ArrowTable>,
    pub pg_constraint: Arc<ArrowTable>,
    pub pg_db_role_setting: Arc<ArrowTable>,
    pub pg_default_acl: Arc<ArrowTable>,
    pub pg_depend: Arc<ArrowTable>,
    pub pg_description: Arc<ArrowTable>,
    pub pg_enum: Arc<ArrowTable>,
    pub pg_event_trigger: Arc<ArrowTable>,
    pub pg_extension: Arc<ArrowTable>,
    pub pg_foreign_data_wrapper: Arc<ArrowTable>,
    pub pg_foreign_server: Arc<ArrowTable>,
    pub pg_foreign_table: Arc<ArrowTable>,
    pub pg_index: Arc<ArrowTable>,
    pub pg_inherits: Arc<ArrowTable>,
    pub pg_init_privs: Arc<ArrowTable>,
    pub pg_largeobject: Arc<ArrowTable>,
    pub pg_largeobject_metadata: Arc<ArrowTable>,
    pub pg_partitioned_table: Arc<ArrowTable>,
    pub pg_policy: Arc<ArrowTable>,
    pub pg_publication: Arc<ArrowTable>,
    pub pg_publication_namespace: Arc<ArrowTable>,
    pub pg_publication_rel: Arc<ArrowTable>,
    pub pg_replication_origin: Arc<ArrowTable>,
    pub pg_rewrite: Arc<ArrowTable>,
    pub pg_seclabel: Arc<ArrowTable>,
    pub pg_sequence: Arc<ArrowTable>,
    pub pg_shdepend: Arc<ArrowTable>,
    pub pg_shdescription: Arc<ArrowTable>,
    pub pg_shseclabel: Arc<ArrowTable>,
    pub pg_statistic: Arc<ArrowTable>,
    pub pg_statistic_ext: Arc<ArrowTable>,
    pub pg_statistic_ext_data: Arc<ArrowTable>,
    pub pg_subscription: Arc<ArrowTable>,
    pub pg_subscription_rel: Arc<ArrowTable>,
    pub pg_tablespace: Arc<ArrowTable>,
    pub pg_trigger: Arc<ArrowTable>,
    pub pg_user_mapping: Arc<ArrowTable>,

    pub pg_get_keywords: Arc<ArrowTable>,
}

impl PgCatalogStaticTables {
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            pg_aggregate: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_aggregate.feather"
                ))
                .to_vec(),
            )?,
            pg_am: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_am.feather"
                ))
                .to_vec(),
            )?,
            pg_amop: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_amop.feather"
                ))
                .to_vec(),
            )?,
            pg_amproc: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_amproc.feather"
                ))
                .to_vec(),
            )?,
            pg_cast: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_cast.feather"
                ))
                .to_vec(),
            )?,
            pg_collation: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_collation.feather"
                ))
                .to_vec(),
            )?,
            pg_conversion: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_conversion.feather"
                ))
                .to_vec(),
            )?,
            pg_language: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_language.feather"
                ))
                .to_vec(),
            )?,
            pg_opclass: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_opclass.feather"
                ))
                .to_vec(),
            )?,
            pg_operator: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_operator.feather"
                ))
                .to_vec(),
            )?,
            pg_opfamily: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_opfamily.feather"
                ))
                .to_vec(),
            )?,
            pg_proc: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_proc.feather"
                ))
                .to_vec(),
            )?,
            pg_range: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_range.feather"
                ))
                .to_vec(),
            )?,
            pg_ts_config: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_ts_config.feather"
                ))
                .to_vec(),
            )?,
            pg_ts_dict: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_ts_dict.feather"
                ))
                .to_vec(),
            )?,
            pg_ts_parser: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_ts_parser.feather"
                ))
                .to_vec(),
            )?,
            pg_ts_template: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_ts_template.feather"
                ))
                .to_vec(),
            )?,
            pg_type: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_type.feather"
                ))
                .to_vec(),
            )?,
            pg_attrdef: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_attrdef.feather"
                ))
                .to_vec(),
            )?,
            pg_auth_members: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_auth_members.feather"
                ))
                .to_vec(),
            )?,
            pg_authid: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_authid.feather"
                ))
                .to_vec(),
            )?,
            pg_constraint: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_constraint.feather"
                ))
                .to_vec(),
            )?,
            pg_db_role_setting: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_db_role_setting.feather"
                ))
                .to_vec(),
            )?,
            pg_default_acl: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_default_acl.feather"
                ))
                .to_vec(),
            )?,
            pg_depend: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_depend.feather"
                ))
                .to_vec(),
            )?,
            pg_description: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_description.feather"
                ))
                .to_vec(),
            )?,
            pg_enum: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_enum.feather"
                ))
                .to_vec(),
            )?,
            pg_event_trigger: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_event_trigger.feather"
                ))
                .to_vec(),
            )?,
            pg_extension: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_extension.feather"
                ))
                .to_vec(),
            )?,
            pg_foreign_data_wrapper: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_foreign_data_wrapper.feather"
                ))
                .to_vec(),
            )?,
            pg_foreign_server: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_foreign_server.feather"
                ))
                .to_vec(),
            )?,
            pg_foreign_table: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_foreign_table.feather"
                ))
                .to_vec(),
            )?,
            pg_index: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_index.feather"
                ))
                .to_vec(),
            )?,
            pg_inherits: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_inherits.feather"
                ))
                .to_vec(),
            )?,
            pg_init_privs: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_init_privs.feather"
                ))
                .to_vec(),
            )?,
            pg_largeobject: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_largeobject.feather"
                ))
                .to_vec(),
            )?,
            pg_largeobject_metadata: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_largeobject_metadata.feather"
                ))
                .to_vec(),
            )?,

            pg_partitioned_table: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_partitioned_table.feather"
                ))
                .to_vec(),
            )?,
            pg_policy: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_policy.feather"
                ))
                .to_vec(),
            )?,
            pg_publication: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_publication.feather"
                ))
                .to_vec(),
            )?,
            pg_publication_namespace: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_publication_namespace.feather"
                ))
                .to_vec(),
            )?,
            pg_publication_rel: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_publication_rel.feather"
                ))
                .to_vec(),
            )?,
            pg_replication_origin: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_replication_origin.feather"
                ))
                .to_vec(),
            )?,
            pg_rewrite: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_rewrite.feather"
                ))
                .to_vec(),
            )?,
            pg_seclabel: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_seclabel.feather"
                ))
                .to_vec(),
            )?,
            pg_sequence: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_sequence.feather"
                ))
                .to_vec(),
            )?,
            pg_shdepend: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_shdepend.feather"
                ))
                .to_vec(),
            )?,
            pg_shdescription: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_shdescription.feather"
                ))
                .to_vec(),
            )?,
            pg_shseclabel: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_shseclabel.feather"
                ))
                .to_vec(),
            )?,
            pg_statistic: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_statistic.feather"
                ))
                .to_vec(),
            )?,
            pg_statistic_ext: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_statistic_ext.feather"
                ))
                .to_vec(),
            )?,
            pg_statistic_ext_data: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_statistic_ext_data.feather"
                ))
                .to_vec(),
            )?,
            pg_subscription: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_subscription.feather"
                ))
                .to_vec(),
            )?,
            pg_subscription_rel: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_subscription_rel.feather"
                ))
                .to_vec(),
            )?,
            pg_tablespace: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_tablespace.feather"
                ))
                .to_vec(),
            )?,
            pg_trigger: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_trigger.feather"
                ))
                .to_vec(),
            )?,
            pg_user_mapping: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_user_mapping.feather"
                ))
                .to_vec(),
            )?,

            pg_get_keywords: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_get_keywords.feather"
                ))
                .to_vec(),
            )?,
        })
    }

    /// Create table from dumped arrow data
    fn create_arrow_table(data_bytes: Vec<u8>) -> Result<Arc<ArrowTable>> {
        ArrowTable::from_ipc_data(data_bytes).map(Arc::new)
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

pub fn create_current_database_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Create a UTF8 array with a single value
        let mut builder = StringBuilder::new();
        builder.append_value("datafusion");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_database",
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
        "pg_get_userbyid",
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
        "pg_table_is_visible",
        vec![DataType::Int32],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_session_user_udf() -> ScalarUDF {
    let func = move |_args: &[ColumnarValue]| {
        let mut builder = StringBuilder::new();
        // TODO: return real user
        builder.append_value("postgres");

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "session_user",
        vec![],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_get_partkeydef_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oid = &args[0];

        let mut builder = StringBuilder::new();
        for _ in 0..oid.len() {
            builder.append_value("");
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_get_partkeydef",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_relation_is_publishable_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oid = &args[0];

        let mut builder = BooleanBuilder::new();
        for _ in 0..oid.len() {
            builder.append_value(true);
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_relation_is_publishable",
        vec![DataType::Int32],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_get_statisticsobjdef_columns_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oid = &args[0];

        let mut builder = StringBuilder::new();
        for _ in 0..oid.len() {
            builder.append_null();
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_get_statisticsobjdef_columns",
        vec![DataType::Int32],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_encoding_to_char_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let encoding = &args[0].as_primitive::<Int32Type>();

        let mut builder = StringBuilder::new();
        for i in 0..encoding.len() {
            if encoding.value(i) == 6 {
                builder.append_value("UTF-8");
            } else {
                builder.append_null();
            }
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_encoding_to_char",
        vec![DataType::Int32],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

const BACKEND_PID: i32 = 1;

pub fn create_pg_backend_pid_udf() -> ScalarUDF {
    let func = move |_args: &[ColumnarValue]| {
        let mut builder = Int32Builder::new();
        builder.append_value(BACKEND_PID);
        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_backend_pid",
        vec![],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_relation_size_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oids = &args[0].as_primitive::<Int32Type>();

        let mut builder = Int32Builder::new();
        for _ in 0..oids.len() {
            builder.append_value(0);
        }

        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_relation_size",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_total_relation_size_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oids = &args[0].as_primitive::<Int32Type>();

        let mut builder = Int32Builder::new();
        for _ in 0..oids.len() {
            builder.append_value(0);
        }

        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_total_relation_size",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_stat_get_numscans() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let index_rel_id = &args[0].as_primitive::<Int32Type>();

        let mut builder = Int32Builder::new();
        for _ in 0..index_rel_id.len() {
            builder.append_value(0);
        }

        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_stat_get_numscans",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_get_constraintdef() -> ScalarUDF {
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct GetConstraintDefUDF {
        signature: Signature,
    }

    impl GetConstraintDefUDF {
        fn new() -> Self {
            let type_signature = TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int32, DataType::Boolean]),
            ]);

            let signature = Signature::new(type_signature, Volatility::Stable);
            GetConstraintDefUDF { signature }
        }
    }

    impl ScalarUDFImpl for GetConstraintDefUDF {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "pg_get_constraintdef"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(
            &self,
            args: datafusion::logical_expr::ScalarFunctionArgs,
        ) -> Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let oids = &args[0].as_primitive::<Int32Type>();

            let mut builder = StringBuilder::new();
            for _ in 0..oids.len() {
                builder.append_value("");
            }

            let array: ArrayRef = Arc::new(builder.finish());
            Ok(ColumnarValue::Array(array))
        }
    }

    GetConstraintDefUDF::new().into()
}

pub fn create_pg_get_partition_ancestors_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let string_array = args[0].as_string::<i32>();

        let mut builder = StringBuilder::new();
        string_array.iter().for_each(|i| builder.append_option(i));
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_partition_ancestors",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

/// Install pg_catalog and postgres UDFs to current `SessionContext`
pub fn setup_pg_catalog<P>(
    session_context: &SessionContext,
    catalog_name: &str,
    context_provider: P,
) -> Result<(), Box<DataFusionError>>
where
    P: PgCatalogContextProvider,
{
    let static_tables = Arc::new(PgCatalogStaticTables::try_new()?);
    let pg_catalog = PgCatalogSchemaProvider::try_new(
        session_context.state().catalog_list().clone(),
        static_tables.clone(),
        context_provider,
    )?;
    session_context
        .catalog(catalog_name)
        .ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Catalog not found when registering pg_catalog: {catalog_name}"
            ))
        })?
        .register_schema("pg_catalog", Arc::new(pg_catalog))?;

    session_context.register_udf(create_current_database_udf());
    session_context.register_udf(create_current_schema_udf());
    session_context.register_udf(create_current_schemas_udf());
    //    session_context.register_udf(create_version_udf());
    session_context.register_udf(create_pg_get_userbyid_udf());
    session_context.register_udf(has_privilege_udf::create_has_privilege_udf(
        "has_table_privilege",
    ));
    session_context.register_udf(has_privilege_udf::create_has_privilege_udf(
        "has_schema_privilege",
    ));
    session_context.register_udf(has_privilege_udf::create_has_privilege_udf(
        "has_database_privilege",
    ));
    session_context.register_udf(has_privilege_udf::create_has_privilege_udf(
        "has_any_column_privilege",
    ));
    session_context.register_udf(create_pg_table_is_visible());
    session_context.register_udf(format_type::create_format_type_udf());
    session_context.register_udf(create_session_user_udf());
    session_context.register_udtf("pg_get_keywords", static_tables.pg_get_keywords.clone());
    session_context.register_udf(pg_get_expr_udf::create_pg_get_expr_udf());
    session_context.register_udf(create_pg_get_partkeydef_udf());
    session_context.register_udf(create_pg_relation_is_publishable_udf());
    session_context.register_udf(create_pg_get_statisticsobjdef_columns_udf());
    session_context.register_udf(create_pg_encoding_to_char_udf());
    session_context.register_udf(create_pg_backend_pid_udf());
    session_context.register_udf(create_pg_relation_size_udf());
    session_context.register_udf(create_pg_total_relation_size_udf());
    session_context.register_udf(create_pg_stat_get_numscans());
    session_context.register_udf(create_pg_get_constraintdef());
    session_context.register_udf(create_pg_get_partition_ancestors_udf());

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_load_arrow_data() {
        let table = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_aggregate.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        assert_eq!(table.schema.fields.len(), 22);
        assert_eq!(table.data.len(), 1);

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_aggregate.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_am.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_amop.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_amproc.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_cast.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_collation.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_conversion.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_language.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_opclass.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_operator.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_opfamily.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_proc.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_range.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_ts_config.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_ts_dict.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_ts_parser.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_ts_template.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_type.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_attrdef.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_auth_members.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_authid.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_constraint.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_db_role_setting.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_default_acl.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_depend.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_description.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_enum.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_event_trigger.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_extension.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_foreign_data_wrapper.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_foreign_server.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_foreign_table.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_index.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_inherits.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_init_privs.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_largeobject.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_largeobject_metadata.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_partitioned_table.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_policy.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_publication.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_publication_namespace.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_publication_rel.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_replication_origin.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_rewrite.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_seclabel.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_sequence.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_shdepend.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_shdescription.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_shseclabel.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_statistic.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_statistic_ext.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_statistic_ext_data.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_subscription.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_subscription_rel.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_tablespace.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_trigger.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_user_mapping.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_get_keywords.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
    }
}
