use crate::domain::model::commit_id::CommitId;
use crate::domain::model::file::File;
use crate::domain::model::stream_id::StreamId;

pub struct Snapshot {
    pub stream_id: StreamId,
    pub commit_id: Option<CommitId>,
    pub files: Vec<File>,
}

impl Snapshot {
    pub fn new(stream_id: StreamId, commit_id: Option<CommitId>, files: Vec<File>) -> Self {
        Self {
            stream_id,
            commit_id,
            files,
        }
    }
}
