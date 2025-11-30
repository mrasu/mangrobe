use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::user_table_stream::UserTablStream;
use chrono::{DateTime, Utc};

pub struct Commit {
    pub id: CommitId,
    pub change_request_id: ChangeRequestId,
    pub stream: UserTablStream,
    pub committed_at: DateTime<Utc>,
}
