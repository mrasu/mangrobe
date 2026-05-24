use crate::domain::model::commit_id::CommitId;
use crate::domain::model::stream::Stream;

pub(crate) struct StreamInfo {
    pub id: Stream,
    pub last_commit_id: CommitId,
}
