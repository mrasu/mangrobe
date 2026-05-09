use crate::domain::model::db_object_identifier::DbObjectIdentifier;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TableIdentifier {
    pub catalog_name: DbObjectIdentifier,
    pub schema_name: DbObjectIdentifier,
    pub table_name: DbObjectIdentifier,
}

impl TableIdentifier {
    pub fn new(
        catalog_name: DbObjectIdentifier,
        schema_name: DbObjectIdentifier,
        table_name: DbObjectIdentifier,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            table_name,
        }
    }

    pub fn full_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.catalog_name.val(),
            self.schema_name.val(),
            self.table_name.val()
        )
    }
}
