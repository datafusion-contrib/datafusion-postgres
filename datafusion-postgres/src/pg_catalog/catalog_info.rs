use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef, catalog::CatalogProviderList, datasource::TableType,
    error::DataFusionError,
};

/// Define the interface for retrieve catalog data for pg_catalog tables
#[async_trait]
pub trait CatalogInfo: Clone + Send + Sync + Debug + 'static {
    fn catalog_names(&self) -> Result<Vec<String>, DataFusionError>;

    fn schema_names(&self, catalog_name: &str) -> Result<Option<Vec<String>>, DataFusionError>;

    fn table_names(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Option<Vec<String>>, DataFusionError>;

    async fn table_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<SchemaRef>, DataFusionError>;

    async fn table_type(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableType>, DataFusionError>;
}

#[async_trait]
impl CatalogInfo for Arc<dyn CatalogProviderList> {
    fn catalog_names(&self) -> Result<Vec<String>, DataFusionError> {
        Ok(CatalogProviderList::catalog_names(self.as_ref()))
    }

    fn schema_names(&self, catalog_name: &str) -> Result<Option<Vec<String>>, DataFusionError> {
        Ok(self.catalog(catalog_name).map(|c| c.schema_names()))
    }

    fn table_names(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Option<Vec<String>>, DataFusionError> {
        Ok(self
            .catalog(catalog_name)
            .and_then(|c| c.schema(schema_name))
            .map(|s| s.table_names()))
    }

    async fn table_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<SchemaRef>, DataFusionError> {
        let schema = self
            .catalog(catalog_name)
            .and_then(|c| c.schema(schema_name));
        if let Some(schema) = schema {
            let table_schema = schema.table(table_name).await?.map(|t| t.schema());
            Ok(table_schema)
        } else {
            Ok(None)
        }
    }

    async fn table_type(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableType>, DataFusionError> {
        let schema = self
            .catalog(catalog_name)
            .and_then(|c| c.schema(schema_name));
        if let Some(schema) = schema {
            let table_type = schema.table_type(table_name).await?;
            Ok(table_type)
        } else {
            Ok(None)
        }
    }
}

pub fn table_type_to_string(tt: &TableType) -> String {
    match tt {
        TableType::Base => "r".to_string(),
        TableType::View => "v".to_string(),
        TableType::Temporary => "r".to_string(),
    }
}
