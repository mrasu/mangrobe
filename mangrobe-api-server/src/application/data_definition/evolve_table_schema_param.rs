use crate::domain::model::table_definition::Column;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) struct EvolveTableSchemaParam {
    pub identifier: TableIdentifier,
    pub proposed_columns: Vec<Column>,
}
