use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;

pub struct GetCurrentSnapshotParam {
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
}
