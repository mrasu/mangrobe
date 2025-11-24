use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use chrono::{DateTime, Utc};

pub struct Commit {
    pub id: CommitId,
    pub change_request_id: ChangeRequestId,
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub committed_at: DateTime<Utc>,
}
