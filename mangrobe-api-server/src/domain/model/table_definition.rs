use crate::domain::model::db_object_identifier::DbObjectIdentifier;
use crate::domain::model::table_identifier::TableIdentifier;
use std::collections::{HashMap, HashSet};
use std::fmt;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum TableDefinitionError {
    #[error("S3 location requires non-empty bucket")]
    S3BucketRequired,

    #[error("partition dst_column must be unset or different from src_column")]
    InvalidPartitionDestination,

    #[error("duplicate column name: '{0}'")]
    DuplicateColumnName(String),

    #[error("partition src_column references unknown column: '{0}'")]
    UnknownPartitionSourceColumn(String),

    #[error("stream references unknown column: '{0}'")]
    UnknownStreamSourceColumn(String),

    #[error("partition dst_column references unknown column: '{0}'")]
    UnknownPartitionDestinationColumn(String),

    #[error("incompatible column definition: '{0}'")]
    IncompatibleColumnDefinition(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableDefinition {
    pub identifier: TableIdentifier,
    pub location: ExternalLocation,
    pub format: FileFormat,
    pub columns: Vec<Column>,
    pub partition_field: PartitionField,
    pub stream_field: StreamField,
    pub comment: Option<String>,
}

impl TableDefinition {
    pub fn new(
        identifier: TableIdentifier,
        location: ExternalLocation,
        format: FileFormat,
        columns: Vec<Column>,
        partition_field: PartitionField,
        stream_field: StreamField,
        comment: Option<String>,
    ) -> Result<Self, TableDefinitionError> {
        let mut column_names = std::collections::HashSet::new();
        for column in &columns {
            if !column_names.insert(column.name.val()) {
                return Err(TableDefinitionError::DuplicateColumnName(column.name.val()));
            }
        }

        if !column_names.contains(&partition_field.src_column.val()) {
            return Err(TableDefinitionError::UnknownPartitionSourceColumn(
                partition_field.src_column.val(),
            ));
        }
        if let Some(dst_column) = &partition_field.dst_column
            && !column_names.contains(&dst_column.val())
        {
            return Err(TableDefinitionError::UnknownPartitionDestinationColumn(
                dst_column.val(),
            ));
        }

        if !column_names.contains(&stream_field.src_column.val()) {
            return Err(TableDefinitionError::UnknownStreamSourceColumn(
                stream_field.src_column.val(),
            ));
        }

        Ok(Self {
            identifier,
            location,
            format,
            columns,
            partition_field,
            stream_field,
            comment,
        })
    }

    pub fn evolve_schema_columns(
        &self,
        proposed_columns: Vec<Column>,
    ) -> Result<(Vec<Column>, bool), TableDefinitionError> {
        let mut proposed_column_names = HashSet::new();
        for column in &proposed_columns {
            if !proposed_column_names.insert(column.name.val()) {
                return Err(TableDefinitionError::DuplicateColumnName(column.name.val()));
            }
        }

        let mut changed = false;
        let mut existing_columns = self.columns.clone();

        let mut column_index_map: HashMap<_, _> = existing_columns
            .iter()
            .enumerate()
            .map(|(idx, column)| (column.name.clone(), idx))
            .collect();

        for proposed_column in proposed_columns {
            match column_index_map.get(&proposed_column.name).copied() {
                Some(idx) => {
                    let existing_column = &mut existing_columns[idx];
                    let column_changed = merge_column(existing_column, proposed_column)?;
                    changed |= column_changed;
                }
                None => {
                    let idx = existing_columns.len();

                    column_index_map.insert(proposed_column.name.clone(), idx);

                    existing_columns.push(proposed_column);
                    changed = true;
                }
            }
        }

        Ok((existing_columns, changed))
    }
}

fn merge_column(
    existing_column: &mut Column,
    proposed_column: Column,
) -> Result<bool, TableDefinitionError> {
    if existing_column == &proposed_column {
        return Ok(false);
    }

    if existing_column.data_type != proposed_column.data_type {
        return Err(TableDefinitionError::IncompatibleColumnDefinition(format!(
            "{} cannot change incompatible data_type",
            proposed_column.name.val()
        )));
    }

    if existing_column.nullable != proposed_column.nullable {
        if existing_column.nullable {
            return Err(TableDefinitionError::IncompatibleColumnDefinition(format!(
                "{} cannot change to non-nullable column",
                proposed_column.name.val()
            )));
        }
        existing_column.nullable = true;
    }

    Ok(true)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExternalLocation {
    pub storage_scheme: StorageScheme,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

impl ExternalLocation {
    pub fn new(
        storage_scheme: StorageScheme,
        bucket: Option<String>,
        prefix: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
    ) -> Result<Self, TableDefinitionError> {
        if matches!(storage_scheme, StorageScheme::S3)
            && bucket.as_ref().is_none_or(String::is_empty)
        {
            return Err(TableDefinitionError::S3BucketRequired);
        }

        Ok(Self {
            storage_scheme,
            bucket,
            prefix,
            endpoint,
            region,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageScheme {
    S3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileFormat {
    Parquet,
    Vortex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Column {
    pub name: DbObjectIdentifier,
    pub data_type: ColumnDataType,
    pub nullable: bool,
    pub comment: Option<String>,
}

impl Column {
    pub fn new(
        name: DbObjectIdentifier,
        data_type: ColumnDataType,
        nullable: bool,
        comment: Option<String>,
    ) -> Self {
        Self {
            name,
            data_type,
            nullable,
            comment,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ColumnDataType {
    Scalar(ScalarType),
    Time(TimeType),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScalarType {
    Bool,
    Int32,
    Int64,
    Float64,
    String,
    Date,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TimeType {
    pub unit: TimeUnit,
}

impl TimeType {
    pub fn new(unit: TimeUnit) -> Self {
        Self { unit }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PartitionField {
    pub src_column: DbObjectIdentifier,
    pub dst_column: Option<DbObjectIdentifier>,
    pub transform: PartitionTransform,
    pub result_type: PartitionDataType,
}

impl PartitionField {
    pub fn new(
        src_column: DbObjectIdentifier,
        dst_column: Option<DbObjectIdentifier>,
        transform: PartitionTransform,
        result_type: PartitionDataType,
    ) -> Result<Self, TableDefinitionError> {
        if dst_column.as_ref() == Some(&src_column) {
            return Err(TableDefinitionError::InvalidPartitionDestination);
        }

        Ok(Self {
            src_column,
            dst_column,
            transform,
            result_type,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PartitionDataType {
    TimeMicrosecond,
    Int64,
}

impl fmt::Display for PartitionDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionDataType::TimeMicrosecond => write!(f, "time_microsecond"),
            PartitionDataType::Int64 => write!(f, "int64"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StreamField {
    pub src_column: DbObjectIdentifier,
    pub dst_column: Option<DbObjectIdentifier>,
    pub transform: PartitionTransform,
    pub result_type: StreamDataType,
}

impl StreamField {
    pub fn new(
        src_column: DbObjectIdentifier,
        dst_column: Option<DbObjectIdentifier>,
        transform: PartitionTransform,
        result_type: StreamDataType,
    ) -> Result<Self, TableDefinitionError> {
        if dst_column.as_ref() == Some(&src_column) {
            return Err(TableDefinitionError::InvalidPartitionDestination);
        }

        Ok(Self {
            src_column,
            dst_column,
            transform,
            result_type,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PartitionTransform {
    Identity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StreamDataType {
    Int64,
}
