use crate::domain::model::table_identifier::TableIdentifier;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableSummary {
    pub identifier: TableIdentifier,
    pub comment: Option<String>,
}
