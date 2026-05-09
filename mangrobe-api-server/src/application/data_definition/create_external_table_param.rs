use crate::domain::model::table_definition::TableDefinition;

pub(crate) struct CreateExternalTableParam {
    pub table: TableDefinition,
    pub skip_if_exists: bool,
}
