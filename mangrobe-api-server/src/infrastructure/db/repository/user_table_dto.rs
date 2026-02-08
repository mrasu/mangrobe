use crate::domain::model::user_table::UserTable;
use crate::domain::model::user_table_name::UserTableName;
use crate::infrastructure::db::entity::user_tables;
use anyhow::bail;

pub(super) fn build_domain_user_table(
    table: &user_tables::Model,
) -> Result<UserTable, anyhow::Error> {
    match UserTableName::try_from(table.name.clone()) {
        Ok(table_name) => Ok(UserTable::new(table.id.into(), table_name)),
        Err(msg) => bail!(msg),
    }
}
