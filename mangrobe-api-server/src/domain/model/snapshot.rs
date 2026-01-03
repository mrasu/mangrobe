use crate::domain::model::commit_id::CommitId;
use crate::domain::model::file::FileWithId;
use crate::domain::model::user_table_stream::UserTablStream;

pub struct Snapshot {
    pub stream: UserTablStream,
    pub commit_id: Option<CommitId>,
    pub files: Vec<FileWithId>,
}

impl Snapshot {
    pub fn new(
        stream: UserTablStream,
        commit_id: Option<CommitId>,
        files: Vec<FileWithId>,
    ) -> Self {
        Self {
            stream,
            commit_id,
            files,
        }
    }
}
