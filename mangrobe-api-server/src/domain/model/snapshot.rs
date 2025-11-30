use crate::domain::model::commit_id::CommitId;
use crate::domain::model::file::File;
use crate::domain::model::user_table_stream::UserTablStream;

pub struct Snapshot {
    pub stream: UserTablStream,
    pub commit_id: Option<CommitId>,
    pub files: Vec<File>,
}

impl Snapshot {
    pub fn new(stream: UserTablStream, commit_id: Option<CommitId>, files: Vec<File>) -> Self {
        Self {
            stream,
            commit_id,
            files,
        }
    }
}
