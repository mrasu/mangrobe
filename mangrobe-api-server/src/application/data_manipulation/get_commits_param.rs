use crate::domain::model::commit_id::CommitId;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) struct GetCommitsParam {
    pub table_identifier: TableIdentifier,
    pub stream_id: StreamId,
    pub commit_id_after: CommitId,
}
