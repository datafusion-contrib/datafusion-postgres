use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::MemTable;
use datafusion::error::Result;

#[derive(Debug, Clone)]
pub(crate) struct PgSettingsView {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
}

impl PgSettingsView {
    pub(crate) fn try_new() -> Result<PgSettingsView> {
        let schema = Arc::new(Schema::new(vec![
            //        name        | setting | unit |                             category                             |                short_
            //desc                |                                                   extra_desc
            //| context | vartype | source  | min_val | max_val | enumvals |
            //boot_val | reset_val | sourcefile | sourceline | pending_restart
            Field::new("name", DataType::Utf8, true),
            Field::new("setting", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            Field::new("category", DataType::Utf8, true),
            Field::new("short_desc", DataType::Utf8, true),
            Field::new("extra_desc", DataType::Utf8, true),
            Field::new("context", DataType::Utf8, true),
            Field::new("vartype", DataType::Utf8, true),
            Field::new("source", DataType::Utf8, true),
            Field::new("min_val", DataType::Utf8, true),
            Field::new("max_val", DataType::Utf8, true),
            Field::new("enumvals", DataType::Utf8, true),
            Field::new("bool_val", DataType::Utf8, true),
            Field::new("reset_val", DataType::Utf8, true),
            Field::new("sourcefile", DataType::Utf8, true),
            Field::new("sourceline", DataType::Int32, true),
            Field::new("pending_restart", DataType::Boolean, true),
        ]));

        let data = Self::create_data(schema.clone())?;

        Ok(Self { schema, data })
    }

    fn create_data(schema: Arc<Schema>) -> Result<Vec<RecordBatch>> {
        let mut name: Vec<Option<&str>> = Vec::new();
        let mut setting: Vec<Option<&str>> = Vec::new();
        let mut unit: Vec<Option<&str>> = Vec::new();
        let mut category: Vec<Option<&str>> = Vec::new();
        let mut short_desc: Vec<Option<&str>> = Vec::new();
        let mut extra_desc: Vec<Option<&str>> = Vec::new();
        let mut context: Vec<Option<&str>> = Vec::new();
        let mut vartype: Vec<Option<&str>> = Vec::new();
        let mut source: Vec<Option<&str>> = Vec::new();
        let mut min_val: Vec<Option<&str>> = Vec::new();
        let mut max_val: Vec<Option<&str>> = Vec::new();
        let mut enumvals: Vec<Option<&str>> = Vec::new();
        let mut bool_val: Vec<Option<&str>> = Vec::new();
        let mut reset_val: Vec<Option<&str>> = Vec::new();
        let mut sourcefile: Vec<Option<&str>> = Vec::new();
        let mut sourceline: Vec<Option<i32>> = Vec::new();
        let mut pending_restart: Vec<Option<bool>> = Vec::new();

        let data = vec![("standard_conforming_strings", "on")];

        for (setting_name, setting_val) in data {
            name.push(Some(setting_name));
            setting.push(Some(setting_val));

            unit.push(None);
            category.push(None);
            short_desc.push(None);
            extra_desc.push(None);
            context.push(None);
            vartype.push(None);
            source.push(None);
            min_val.push(None);
            max_val.push(None);
            enumvals.push(None);
            bool_val.push(None);
            reset_val.push(None);
            sourcefile.push(None);
            sourceline.push(None);
            pending_restart.push(None);
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(name)),
            Arc::new(StringArray::from(setting)),
            Arc::new(StringArray::from(unit)),
            Arc::new(StringArray::from(category)),
            Arc::new(StringArray::from(short_desc)),
            Arc::new(StringArray::from(extra_desc)),
            Arc::new(StringArray::from(context)),
            Arc::new(StringArray::from(vartype)),
            Arc::new(StringArray::from(source)),
            Arc::new(StringArray::from(min_val)),
            Arc::new(StringArray::from(max_val)),
            Arc::new(StringArray::from(enumvals)),
            Arc::new(StringArray::from(bool_val)),
            Arc::new(StringArray::from(reset_val)),
            Arc::new(StringArray::from(sourcefile)),
            Arc::new(Int32Array::from(sourceline)),
            Arc::new(BooleanArray::from(pending_restart)),
        ];

        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        Ok(vec![batch])
    }

    pub(crate) fn try_into_memtable(self) -> Result<MemTable> {
        MemTable::try_new(self.schema, vec![self.data])
    }
}
