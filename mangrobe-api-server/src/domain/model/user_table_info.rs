use crate::domain::model::table_definition::PartitionDataType;
use crate::domain::model::user_table_id::UserTableId;

#[derive(Clone, Debug)]
pub(crate) struct UserTableInfo {
    pub id: UserTableId,
    pub partition_data_type: PartitionDataType,
}
