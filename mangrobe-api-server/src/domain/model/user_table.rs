use crate::domain::model::user_table_id::UserTableId;
use crate::domain::model::user_table_name::UserTableName;

pub struct UserTable {
    pub id: UserTableId,
    pub name: UserTableName,
}

impl UserTable {
    pub fn new(id: UserTableId, name: UserTableName) -> Self {
        Self { id, name }
    }
}
