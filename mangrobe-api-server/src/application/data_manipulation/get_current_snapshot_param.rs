use crate::domain::model::user_table_stream::UserTablStream;

pub struct GetCurrentSnapshotParam {
    pub stream: UserTablStream,
}
