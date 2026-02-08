use crate::domain::model::commit_id::CommitId;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_name::UserTableName;

pub struct GetChangesParam {
    pub table_name: UserTableName,
    pub stream_id: StreamId,
    pub commit_id_after: CommitId,
}
