use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;

#[derive(Clone, Debug)]
pub struct UserTablStream {
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
}

impl UserTablStream {
    pub fn new(user_table_id: UserTableId, stream_id: StreamId) -> Self {
        Self {
            user_table_id,
            stream_id,
        }
    }
}
