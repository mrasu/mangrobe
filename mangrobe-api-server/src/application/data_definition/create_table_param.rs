use crate::domain::model::user_table_name::UserTableName;

pub(crate) struct CreateTableParam {
    pub table_name: UserTableName,
    pub skip_if_exists: bool,
}
