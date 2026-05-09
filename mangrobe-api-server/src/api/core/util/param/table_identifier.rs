use crate::api::core::util::error::ParameterError;
use crate::domain::model::table_definition::{DbObjectIdentifier, TableIdentifier};

pub(crate) fn to_table_identifier(
    identifier: &crate::api::grpc::proto::TableIdentifier,
) -> Result<TableIdentifier, ParameterError> {
    Ok(TableIdentifier::new(
        to_db_object_identifier("identifier.catalog_name", identifier.catalog_name.clone())?,
        to_db_object_identifier("identifier.schema_name", identifier.schema_name.clone())?,
        to_db_object_identifier("identifier.table_name", identifier.table_name.clone())?,
    ))
}

pub(crate) fn to_db_object_identifier(
    key: &str,
    value: String,
) -> Result<DbObjectIdentifier, ParameterError> {
    DbObjectIdentifier::try_from(value)
        .map_err(|err| ParameterError::Invalid(key.to_owned(), err.to_string()))
}
