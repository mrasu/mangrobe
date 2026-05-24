use crate::domain::model::commit_id::CommitId;
use crate::domain::model::file::FileWithId;
use crate::domain::model::table_definition::PartitionDataType;
use crate::domain::model::user_table_stream::UserTablStream;

#[allow(dead_code)]
pub(crate) struct Snapshot {
    pub stream: UserTablStream,
    pub commit_id: Option<CommitId>,
    pub files: Vec<FileWithId>,
    pub partition_data_type: PartitionDataType,
}

impl Snapshot {
    pub fn new(
        stream: UserTablStream,
        commit_id: Option<CommitId>,
        files: Vec<FileWithId>,
        partition_data_type: PartitionDataType,
    ) -> Self {
        Self {
            stream,
            commit_id,
            files,
            partition_data_type,
        }
    }
}
