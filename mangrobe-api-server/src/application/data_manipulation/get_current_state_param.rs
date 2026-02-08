use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_name::UserTableName;

pub struct GetCurrentStateParam {
    pub table_name: UserTableName,
    pub stream_id: StreamId,
}
