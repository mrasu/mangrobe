pub(crate) use crate::domain::model::change_request_file_data::ChangeRequestFileData;
use crate::domain::model::change_request_file_entry::ChangeRequestFileEntry;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::stream_id::StreamId;

pub struct CommittedChangeRequest {
    pub commit_id: CommitId,
    pub file_entry: ChangeRequestFileEntry,
}

pub struct CommittedStreamChange {
    pub stream_id: StreamId,
    pub committed_changes: Vec<CommittedChangeRequestData>,
}

impl CommittedStreamChange {
    pub fn new(stream_id: StreamId, committed_changes: Vec<CommittedChangeRequestData>) -> Self {
        Self {
            stream_id,
            committed_changes,
        }
    }
}

pub struct CommittedChangeRequestData {
    pub commit_id: CommitId,
    pub file_data: ChangeRequestFileData,
}
