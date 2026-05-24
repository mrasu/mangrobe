use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::table_definition::to_column;
use crate::api::core::util::param::table_identifier::{
    to_db_object_identifier, to_table_identifier,
};
use crate::api::core::util::param_util::{invalid_enum, required};
use crate::api::grpc::proto::{
    CreateTableRequest, ExternalLocation as ProtoExternalLocation, FileFormat as ProtoFileFormat,
    PartitionDataType as ProtoPartitionDataType, PartitionTransform as ProtoPartitionTransform,
    StorageScheme as ProtoStorageScheme, StreamDataType as ProtoStreamDataType,
    TableDefinition as ProtoTableDefinition,
};
use crate::application::data_definition::create_table_param::CreateTableParam;
use crate::domain::model::table_definition::{
    ExternalLocation, FileFormat, PartitionDataType, PartitionField, PartitionTransform,
    StorageScheme, StreamDataType, StreamField, TableDefinition,
};

pub(crate) fn build_create_table_param(
    req: &CreateTableRequest,
) -> Result<CreateTableParam, ParameterError> {
    Ok(CreateTableParam {
        table: to_table_definition(required("table", req.table.as_ref())?)?,
        skip_if_exists: req.skip_if_exists,
    })
}

fn to_table_definition(table: &ProtoTableDefinition) -> Result<TableDefinition, ParameterError> {
    TableDefinition::new(
        to_table_identifier(required("identifier", table.identifier.as_ref())?)?,
        to_external_location(required("location", table.location.as_ref())?)?,
        to_file_format(table.format, "format")?,
        table
            .columns
            .iter()
            .map(to_column)
            .collect::<Result<Vec<_>, _>>()?,
        to_partition_field(required("partition", table.partition_field.as_ref())?)?,
        to_stream_field(required("stream", table.stream_field.as_ref())?)?,
        table.comment.clone(),
    )
    .map_err(|err| ParameterError::Invalid("table".to_owned(), err.to_string()))
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

fn to_partition_field(
    field: &crate::api::grpc::proto::PartitionField,
) -> Result<PartitionField, ParameterError> {
    PartitionField::new(
        to_db_object_identifier("partition_field.src_column", field.src_column.clone())?,
        field
            .dst_column
            .clone()
            .map(|dst_column| to_db_object_identifier("partition_field.dst_column", dst_column))
            .transpose()?,
        to_partition_transform(field.transform, "partition_field.transform")?,
        to_partition_data_type(field.result_type, "partition_field.result_type")?,
    )
    .map_err(|err| ParameterError::Invalid("partition_field".to_owned(), err.to_string()))
}

fn to_stream_field(
    field: &crate::api::grpc::proto::StreamField,
) -> Result<StreamField, ParameterError> {
    StreamField::new(
        to_db_object_identifier("stream_field.src_column", field.src_column.clone())?,
        field
            .dst_column
            .clone()
            .map(|dst_column| to_db_object_identifier("stream_field.dst_column", dst_column))
            .transpose()?,
        to_partition_transform(field.transform, "stream_field.transform")?,
        to_stream_data_type(field.result_type, "stream_field.result_type")?,
    )
    .map_err(|err| ParameterError::Invalid("stream_field".to_owned(), err.to_string()))
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

fn to_partition_transform(value: i32, key: &str) -> Result<PartitionTransform, ParameterError> {
    match ProtoPartitionTransform::try_from(value) {
        Ok(ProtoPartitionTransform::Identity) => Ok(PartitionTransform::Identity),
        Ok(ProtoPartitionTransform::Unspecified) | Err(_) => invalid_enum(key),
    }
}

fn to_partition_data_type(value: i32, key: &str) -> Result<PartitionDataType, ParameterError> {
    match ProtoPartitionDataType::try_from(value) {
        Ok(ProtoPartitionDataType::TimeMicrosecond) => Ok(PartitionDataType::TimeMicrosecond),
        Ok(ProtoPartitionDataType::Int64) => Ok(PartitionDataType::Int64),
        Ok(ProtoPartitionDataType::Unspecified) | Err(_) => invalid_enum(key),
    }
}

fn to_stream_data_type(value: i32, key: &str) -> Result<StreamDataType, ParameterError> {
    match ProtoStreamDataType::try_from(value) {
        Ok(ProtoStreamDataType::Int64) => Ok(StreamDataType::Int64),
        Ok(ProtoStreamDataType::Unspecified) | Err(_) => invalid_enum(key),
    }
}
