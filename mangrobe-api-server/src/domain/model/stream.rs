use crate::domain::model::commit_id::CommitId;
use crate::domain::model::stream_id::StreamId;

pub struct Stream {
    pub id: StreamId,
    pub last_commit_id: CommitId,
}
