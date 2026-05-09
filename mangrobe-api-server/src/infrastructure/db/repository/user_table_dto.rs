use crate::domain::model::db_object_identifier::DbObjectIdentifier;
use crate::domain::model::table_definition::{
    Column, ColumnDataType, ExternalLocation, FileFormat, PartitionField, PartitionTransform,
    ScalarType, StorageScheme, TableDefinition, TimeType, TimeUnit,
};
use crate::domain::model::table_identifier::TableIdentifier;
use crate::domain::model::table_summary::TableSummary;
use crate::domain::model::user_table::UserTable;
use crate::domain::model::user_table_name::UserTableName;
use crate::infrastructure::db::entity::user_tables::{self, ActiveModel};
use anyhow::bail;
use sea_orm::ActiveValue::Set;
use serde::{Deserialize, Serialize};

pub(super) fn build_domain_user_table(
    table: &user_tables::Model,
) -> Result<UserTable, anyhow::Error> {
    match UserTableName::try_from(table.name.clone()) {
        Ok(table_name) => Ok(UserTable::new(table.id.into(), table_name)),
        Err(msg) => bail!(msg),
    }
}

pub(super) fn build_domain_table_definition(
    table: &user_tables::Model,
) -> Result<TableDefinition, anyhow::Error> {
    let location: LocationDto = serde_json::from_value(table.location.clone())?;
    let columns: Vec<ColumnDto> = serde_json::from_value(table.columns.clone())?;
    let partition_fields: Vec<PartitionFieldDto> =
        serde_json::from_value(table.partitions.clone())?;

    Ok(TableDefinition::new(
        TableIdentifier::new(
            to_db_object_identifier(table.catalog_name.clone())?,
            to_db_object_identifier(table.schema_name.clone())?,
            to_db_object_identifier(table.name.clone())?,
        ),
        location.try_into()?,
        to_file_format(table.format)?,
        columns
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?,
        partition_fields
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?,
        table.comment.clone(),
    )?)
}

pub(super) fn build_domain_table_summary(
    catalog_name: String,
    schema_name: String,
    table_name: String,
    comment: Option<String>,
) -> Result<TableSummary, anyhow::Error> {
    Ok(TableSummary {
        identifier: TableIdentifier::new(
            to_db_object_identifier(catalog_name)?,
            to_db_object_identifier(schema_name)?,
            to_db_object_identifier(table_name)?,
        ),
        comment,
    })
}

pub(super) fn build_active_model(table: &TableDefinition) -> ActiveModel {
    ActiveModel {
        id: Default::default(),
        catalog_name: Set(table.identifier.catalog_name.val()),
        schema_name: Set(table.identifier.schema_name.val()),
        name: Set(table.identifier.table_name.val()),
        location: Set(serde_json::to_value(LocationDto::from(&table.location))
            .expect("location DTO should serialize")),
        format: Set(file_format_to_i32(table.format)),
        columns: Set(build_columns_value(&table.columns)),
        partitions: Set(serde_json::to_value(
            table
                .partition_fields
                .iter()
                .map(PartitionFieldDto::from)
                .collect::<Vec<_>>(),
        )
        .expect("partition field DTO should serialize")),
        comment: Set(table.comment.clone()),
        created_at: Default::default(),
        updated_at: Default::default(),
    }
}

pub(super) fn build_columns_value(columns: &[Column]) -> serde_json::Value {
    serde_json::to_value(columns.iter().map(ColumnDto::from).collect::<Vec<_>>())
        .expect("column DTO should serialize")
}

fn to_db_object_identifier(value: String) -> Result<DbObjectIdentifier, anyhow::Error> {
    value.try_into().map_err(Into::into)
}

fn to_file_format(value: i32) -> Result<FileFormat, anyhow::Error> {
    match value {
        1 => Ok(FileFormat::Parquet),
        2 => Ok(FileFormat::Vortex),
        _ => bail!("unsupported file format: {}", value),
    }
}

fn file_format_to_i32(format: FileFormat) -> i32 {
    match format {
        FileFormat::Parquet => 1,
        FileFormat::Vortex => 2,
    }
}

#[derive(Serialize, Deserialize)]
struct LocationDto {
    storage_scheme: StorageSchemeDto,
    bucket: Option<String>,
    prefix: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
}

impl From<&ExternalLocation> for LocationDto {
    fn from(location: &ExternalLocation) -> Self {
        Self {
            storage_scheme: StorageSchemeDto::from(location.storage_scheme),
            bucket: location.bucket.clone(),
            prefix: location.prefix.clone(),
            endpoint: location.endpoint.clone(),
            region: location.region.clone(),
        }
    }
}

impl TryFrom<LocationDto> for ExternalLocation {
    type Error = anyhow::Error;

    fn try_from(value: LocationDto) -> Result<Self, Self::Error> {
        Ok(ExternalLocation::new(
            value.storage_scheme.into(),
            value.bucket,
            value.prefix,
            value.endpoint,
            value.region,
        )?)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StorageSchemeDto {
    S3,
}

impl From<StorageScheme> for StorageSchemeDto {
    fn from(value: StorageScheme) -> Self {
        match value {
            StorageScheme::S3 => Self::S3,
        }
    }
}

impl From<StorageSchemeDto> for StorageScheme {
    fn from(value: StorageSchemeDto) -> Self {
        match value {
            StorageSchemeDto::S3 => Self::S3,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ColumnDto {
    name: String,
    data_type: DataTypeDto,
    nullable: bool,
    comment: Option<String>,
}

impl From<&Column> for ColumnDto {
    fn from(column: &Column) -> Self {
        Self {
            name: column.name.val(),
            data_type: DataTypeDto::from(&column.data_type),
            nullable: column.nullable,
            comment: column.comment.clone(),
        }
    }
}

impl TryFrom<ColumnDto> for Column {
    type Error = anyhow::Error;

    fn try_from(value: ColumnDto) -> Result<Self, Self::Error> {
        Ok(Column::new(
            to_db_object_identifier(value.name)?,
            value.data_type.try_into()?,
            value.nullable,
            value.comment,
        ))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DataTypeDto {
    Scalar { scalar: ScalarTypeDto },
    Time { unit: TimeUnitDto },
}

impl From<&ColumnDataType> for DataTypeDto {
    fn from(value: &ColumnDataType) -> Self {
        match value {
            ColumnDataType::Scalar(scalar) => Self::Scalar {
                scalar: ScalarTypeDto::from(*scalar),
            },
            ColumnDataType::Time(time) => Self::Time {
                unit: TimeUnitDto::from(time.unit),
            },
        }
    }
}

impl TryFrom<DataTypeDto> for ColumnDataType {
    type Error = anyhow::Error;

    fn try_from(value: DataTypeDto) -> Result<Self, Self::Error> {
        match value {
            DataTypeDto::Scalar { scalar } => Ok(Self::Scalar(scalar.into())),
            DataTypeDto::Time { unit } => Ok(Self::Time(TimeType::new(unit.into()))),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScalarTypeDto {
    Bool,
    Int64,
    Float64,
    String,
    Date,
}

impl From<ScalarType> for ScalarTypeDto {
    fn from(value: ScalarType) -> Self {
        match value {
            ScalarType::Bool => Self::Bool,
            ScalarType::Int64 => Self::Int64,
            ScalarType::Float64 => Self::Float64,
            ScalarType::String => Self::String,
            ScalarType::Date => Self::Date,
        }
    }
}

impl From<ScalarTypeDto> for ScalarType {
    fn from(value: ScalarTypeDto) -> Self {
        match value {
            ScalarTypeDto::Bool => Self::Bool,
            ScalarTypeDto::Int64 => Self::Int64,
            ScalarTypeDto::Float64 => Self::Float64,
            ScalarTypeDto::String => Self::String,
            ScalarTypeDto::Date => Self::Date,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TimeUnitDto {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl From<TimeUnit> for TimeUnitDto {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

impl From<TimeUnitDto> for TimeUnit {
    fn from(value: TimeUnitDto) -> Self {
        match value {
            TimeUnitDto::Second => Self::Second,
            TimeUnitDto::Millisecond => Self::Millisecond,
            TimeUnitDto::Microsecond => Self::Microsecond,
            TimeUnitDto::Nanosecond => Self::Nanosecond,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct PartitionFieldDto {
    src_column: String,
    dst_column: Option<String>,
    transform: PartitionTransformDto,
    result_type: DataTypeDto,
}

impl From<&PartitionField> for PartitionFieldDto {
    fn from(field: &PartitionField) -> Self {
        Self {
            src_column: field.src_column.val(),
            dst_column: field.dst_column.as_ref().map(DbObjectIdentifier::val),
            transform: PartitionTransformDto::from(field.transform),
            result_type: DataTypeDto::from(&field.result_type),
        }
    }
}

impl TryFrom<PartitionFieldDto> for PartitionField {
    type Error = anyhow::Error;

    fn try_from(value: PartitionFieldDto) -> Result<Self, Self::Error> {
        Ok(PartitionField::new(
            to_db_object_identifier(value.src_column)?,
            value.dst_column.map(to_db_object_identifier).transpose()?,
            value.transform.into(),
            value.result_type.try_into()?,
        )?)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PartitionTransformDto {
    Identity,
    Hour,
    Day,
    Month,
    Year,
}

impl From<PartitionTransform> for PartitionTransformDto {
    fn from(value: PartitionTransform) -> Self {
        match value {
            PartitionTransform::Identity => Self::Identity,
            PartitionTransform::Hour => Self::Hour,
            PartitionTransform::Day => Self::Day,
            PartitionTransform::Month => Self::Month,
            PartitionTransform::Year => Self::Year,
        }
    }
}

impl From<PartitionTransformDto> for PartitionTransform {
    fn from(value: PartitionTransformDto) -> Self {
        match value {
            PartitionTransformDto::Identity => Self::Identity,
            PartitionTransformDto::Hour => Self::Hour,
            PartitionTransformDto::Day => Self::Day,
            PartitionTransformDto::Month => Self::Month,
            PartitionTransformDto::Year => Self::Year,
        }
    }
}
