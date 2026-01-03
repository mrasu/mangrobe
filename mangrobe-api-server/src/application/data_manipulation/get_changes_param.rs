use crate::domain::model::commit_id::CommitId;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;

pub struct GetChangesParam {
    pub table_id: UserTableId,
    pub stream_id: StreamId,
    pub commit_id_after: CommitId,
}
