use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::table_identifier::to_db_object_identifier;
use crate::api::core::util::param::table_identifier::to_proto_table_identifier;
use crate::api::core::util::param_util::{invalid_enum, required};
use crate::api::grpc::proto::{
    Column as ProtoColumn, DataType as ProtoDataType, ExternalLocation as ProtoExternalLocation,
    FileFormat as ProtoFileFormat, PartitionField as ProtoPartitionField,
    PartitionTransform as ProtoPartitionTransform, ScalarType as ProtoScalarType,
    StorageScheme as ProtoStorageScheme, TableDefinition as ProtoTableDefinition,
    TimeType as ProtoTimeType, TimeUnit as ProtoTimeUnit, data_type,
};
use crate::domain::model::table_definition::{
    Column, ColumnDataType, ExternalLocation, FileFormat, PartitionField, PartitionTransform,
    ScalarType, StorageScheme, TableDefinition, TimeType, TimeUnit,
};

pub(crate) fn to_column(column: &ProtoColumn) -> Result<Column, ParameterError> {
    Ok(Column::new(
        to_db_object_identifier("columns.name", column.name.clone())?,
        to_column_data_type(required("columns.data_type", column.data_type.as_ref())?)?,
        column.nullable,
        column.comment.clone(),
    ))
}

pub(crate) fn to_column_data_type(
    data_type: &ProtoDataType,
) -> Result<ColumnDataType, ParameterError> {
    match required("data_type.type", data_type.r#type.as_ref())? {
        data_type::Type::Scalar(value) => Ok(ColumnDataType::Scalar(to_scalar_type(*value)?)),
        data_type::Type::Time(time) => Ok(ColumnDataType::Time(TimeType::new(to_time_unit(
            time.unit,
            "data_type.time.unit",
        )?))),
    }
}

fn to_scalar_type(value: i32) -> Result<ScalarType, ParameterError> {
    match ProtoScalarType::try_from(value) {
        Ok(ProtoScalarType::Bool) => Ok(ScalarType::Bool),
        Ok(ProtoScalarType::Int32) => Ok(ScalarType::Int32),
        Ok(ProtoScalarType::Int64) => Ok(ScalarType::Int64),
        Ok(ProtoScalarType::Float64) => Ok(ScalarType::Float64),
        Ok(ProtoScalarType::String) => Ok(ScalarType::String),
        Ok(ProtoScalarType::Date) => Ok(ScalarType::Date),
        Ok(ProtoScalarType::Unspecified) | Err(_) => invalid_enum("data_type.scalar"),
    }
}

fn to_time_unit(value: i32, key: &str) -> Result<TimeUnit, ParameterError> {
    match ProtoTimeUnit::try_from(value) {
        Ok(ProtoTimeUnit::Second) => Ok(TimeUnit::Second),
        Ok(ProtoTimeUnit::Millisecond) => Ok(TimeUnit::Millisecond),
        Ok(ProtoTimeUnit::Microsecond) => Ok(TimeUnit::Microsecond),
        Ok(ProtoTimeUnit::Nanosecond) => Ok(TimeUnit::Nanosecond),
        Ok(ProtoTimeUnit::Unspecified) | Err(_) => invalid_enum(key),
    }
}

pub(crate) fn to_proto_table_definition(table: TableDefinition) -> ProtoTableDefinition {
    ProtoTableDefinition {
        identifier: Some(to_proto_table_identifier(table.identifier)),
        location: Some(to_proto_external_location(table.location)),
        format: to_proto_file_format(table.format) as i32,
        columns: table.columns.into_iter().map(to_proto_column).collect(),
        partition_fields: table
            .partition_fields
            .into_iter()
            .map(to_proto_partition_field)
            .collect(),
        comment: table.comment,
    }
}

fn to_proto_external_location(location: ExternalLocation) -> ProtoExternalLocation {
    ProtoExternalLocation {
        storage_scheme: to_proto_storage_scheme(location.storage_scheme) as i32,
        bucket: location.bucket,
        prefix: location.prefix,
        endpoint: location.endpoint,
        region: location.region,
    }
}

fn to_proto_column(column: Column) -> ProtoColumn {
    ProtoColumn {
        name: column.name.val(),
        data_type: Some(to_proto_data_type(column.data_type)),
        nullable: column.nullable,
        comment: column.comment,
    }
}

fn to_proto_partition_field(field: PartitionField) -> ProtoPartitionField {
    ProtoPartitionField {
        src_column: field.src_column.val(),
        dst_column: field.dst_column.map(|dst_column| dst_column.val()),
        transform: to_proto_partition_transform(field.transform) as i32,
        result_type: Some(to_proto_data_type(field.result_type)),
    }
}

fn to_proto_data_type(data_type: ColumnDataType) -> ProtoDataType {
    ProtoDataType {
        r#type: Some(match data_type {
            ColumnDataType::Scalar(scalar) => {
                data_type::Type::Scalar(to_proto_scalar_type(scalar) as i32)
            }
            ColumnDataType::Time(time) => data_type::Type::Time(to_proto_time_type(time)),
        }),
    }
}

fn to_proto_time_type(time: TimeType) -> ProtoTimeType {
    ProtoTimeType {
        unit: to_proto_time_unit(time.unit) as i32,
    }
}

fn to_proto_storage_scheme(storage_scheme: StorageScheme) -> ProtoStorageScheme {
    match storage_scheme {
        StorageScheme::S3 => ProtoStorageScheme::S3,
    }
}

fn to_proto_file_format(format: FileFormat) -> ProtoFileFormat {
    match format {
        FileFormat::Parquet => ProtoFileFormat::Parquet,
        FileFormat::Vortex => ProtoFileFormat::Vortex,
    }
}

fn to_proto_scalar_type(scalar: ScalarType) -> ProtoScalarType {
    match scalar {
        ScalarType::Bool => ProtoScalarType::Bool,
        ScalarType::Int32 => ProtoScalarType::Int32,
        ScalarType::Int64 => ProtoScalarType::Int64,
        ScalarType::Float64 => ProtoScalarType::Float64,
        ScalarType::String => ProtoScalarType::String,
        ScalarType::Date => ProtoScalarType::Date,
    }
}

fn to_proto_time_unit(unit: TimeUnit) -> ProtoTimeUnit {
    match unit {
        TimeUnit::Second => ProtoTimeUnit::Second,
        TimeUnit::Millisecond => ProtoTimeUnit::Millisecond,
        TimeUnit::Microsecond => ProtoTimeUnit::Microsecond,
        TimeUnit::Nanosecond => ProtoTimeUnit::Nanosecond,
    }
}

fn to_proto_partition_transform(transform: PartitionTransform) -> ProtoPartitionTransform {
    match transform {
        PartitionTransform::Identity => ProtoPartitionTransform::Identity,
        PartitionTransform::Hour => ProtoPartitionTransform::Hour,
        PartitionTransform::Day => ProtoPartitionTransform::Day,
        PartitionTransform::Month => ProtoPartitionTransform::Month,
        PartitionTransform::Year => ProtoPartitionTransform::Year,
    }
}
