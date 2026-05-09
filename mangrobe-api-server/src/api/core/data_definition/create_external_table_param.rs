use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::{
    CreateExternalTableRequest, DataType as ProtoDataType,
    ExternalLocation as ProtoExternalLocation, FileFormat as ProtoFileFormat,
    PartitionTransform as ProtoPartitionTransform, ScalarType as ProtoScalarType,
    StorageScheme as ProtoStorageScheme, TableDefinition as ProtoTableDefinition,
    TableIdentifier as ProtoTableIdentifier, TimeUnit as ProtoTimeUnit, data_type,
};
use crate::application::data_definition::create_external_table_param::CreateExternalTableParam;
use crate::domain::model::table_definition::{
    Column, DataType, DbObjectIdentifier, ExternalLocation, FileFormat, PartitionField,
    PartitionTransform, ScalarType, StorageScheme, TableDefinition, TableIdentifier, TimeType,
    TimeUnit,
};

pub(crate) fn build_create_external_table_param(
    req: &CreateExternalTableRequest,
) -> Result<CreateExternalTableParam, ParameterError> {
    Ok(CreateExternalTableParam {
        table: to_table_definition(required(req.table.as_ref(), "table")?)?,
        skip_if_exists: req.skip_if_exists,
    })
}

fn to_table_definition(table: &ProtoTableDefinition) -> Result<TableDefinition, ParameterError> {
    TableDefinition::new(
        to_table_identifier(required(table.identifier.as_ref(), "identifier")?)?,
        to_external_location(required(table.location.as_ref(), "location")?)?,
        to_file_format(table.format, "format")?,
        table
            .columns
            .iter()
            .map(to_column)
            .collect::<Result<Vec<_>, _>>()?,
        table
            .partition_fields
            .iter()
            .map(to_partition_field)
            .collect::<Result<Vec<_>, _>>()?,
        table.comment.clone(),
    )
    .map_err(|err| ParameterError::Invalid("table".to_owned(), err.to_string()))
}

fn to_table_identifier(
    identifier: &ProtoTableIdentifier,
) -> Result<TableIdentifier, ParameterError> {
    Ok(TableIdentifier::new(
        to_db_object_identifier("identifier.catalog_name", identifier.catalog_name.clone())?,
        to_db_object_identifier("identifier.schema_name", identifier.schema_name.clone())?,
        to_db_object_identifier("identifier.table_name", identifier.table_name.clone())?,
    ))
}

fn to_external_location(
    location: &ProtoExternalLocation,
) -> Result<ExternalLocation, ParameterError> {
    ExternalLocation::new(
        to_storage_scheme(location.storage_scheme, "location.storage_scheme")?,
        location.bucket.clone(),
        location.prefix.clone(),
        location.endpoint.clone(),
        location.region.clone(),
    )
    .map_err(|err| ParameterError::Invalid("location".to_owned(), err.to_string()))
}

fn to_column(column: &crate::api::grpc::proto::Column) -> Result<Column, ParameterError> {
    Ok(Column::new(
        to_db_object_identifier("columns.name", column.name.clone())?,
        to_data_type(required(column.data_type.as_ref(), "columns.data_type")?)?,
        column.nullable,
        column.comment.clone(),
    ))
}

fn to_partition_field(
    field: &crate::api::grpc::proto::PartitionField,
) -> Result<PartitionField, ParameterError> {
    PartitionField::new(
        to_db_object_identifier("partition_fields.src_column", field.src_column.clone())?,
        field
            .dst_column
            .clone()
            .map(|dst_column| to_db_object_identifier("partition_fields.dst_column", dst_column))
            .transpose()?,
        to_partition_transform(field.transform, "partition_fields.transform")?,
        to_data_type(required(
            field.result_type.as_ref(),
            "partition_fields.result_type",
        )?)?,
    )
    .map_err(|err| ParameterError::Invalid("partition_fields".to_owned(), err.to_string()))
}

fn to_data_type(data_type: &ProtoDataType) -> Result<DataType, ParameterError> {
    match required(data_type.r#type.as_ref(), "data_type.type")? {
        data_type::Type::Scalar(value) => Ok(DataType::Scalar(to_scalar_type(*value)?)),
        data_type::Type::Time(time) => Ok(DataType::Time(TimeType::new(to_time_unit(
            time.unit,
            "data_type.time.unit",
        )?))),
    }
}

fn to_db_object_identifier(key: &str, value: String) -> Result<DbObjectIdentifier, ParameterError> {
    DbObjectIdentifier::try_from(value)
        .map_err(|err| ParameterError::Invalid(key.to_owned(), err.to_string()))
}

fn to_storage_scheme(value: i32, key: &str) -> Result<StorageScheme, ParameterError> {
    match ProtoStorageScheme::try_from(value) {
        Ok(ProtoStorageScheme::S3) => Ok(StorageScheme::S3),
        Ok(ProtoStorageScheme::Unspecified) | Err(_) => invalid_enum(key),
    }
}

fn to_file_format(value: i32, key: &str) -> Result<FileFormat, ParameterError> {
    match ProtoFileFormat::try_from(value) {
        Ok(ProtoFileFormat::Parquet) => Ok(FileFormat::Parquet),
        Ok(ProtoFileFormat::Vortex) => Ok(FileFormat::Vortex),
        Ok(ProtoFileFormat::Unspecified) | Err(_) => invalid_enum(key),
    }
}

fn to_scalar_type(value: i32) -> Result<ScalarType, ParameterError> {
    match ProtoScalarType::try_from(value) {
        Ok(ProtoScalarType::Bool) => Ok(ScalarType::Bool),
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

fn to_partition_transform(value: i32, key: &str) -> Result<PartitionTransform, ParameterError> {
    match ProtoPartitionTransform::try_from(value) {
        Ok(ProtoPartitionTransform::Identity) => Ok(PartitionTransform::Identity),
        Ok(ProtoPartitionTransform::Hour) => Ok(PartitionTransform::Hour),
        Ok(ProtoPartitionTransform::Day) => Ok(PartitionTransform::Day),
        Ok(ProtoPartitionTransform::Month) => Ok(PartitionTransform::Month),
        Ok(ProtoPartitionTransform::Year) => Ok(PartitionTransform::Year),
        Ok(ProtoPartitionTransform::Unspecified) | Err(_) => invalid_enum(key),
    }
}

fn required<'a, T>(value: Option<&'a T>, key: &str) -> Result<&'a T, ParameterError> {
    value.ok_or_else(|| ParameterError::Required(key.to_owned()))
}

fn invalid_enum<T>(key: &str) -> Result<T, ParameterError> {
    Err(ParameterError::Invalid(
        key.to_owned(),
        "unsupported or unspecified enum value".to_owned(),
    ))
}
