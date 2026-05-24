use crate::domain::model::table_definition::TableDefinition;

pub(crate) struct CreateTableParam {
    pub table: TableDefinition,
    pub skip_if_exists: bool,
}
