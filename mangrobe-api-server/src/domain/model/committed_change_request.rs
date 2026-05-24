pub(crate) use crate::domain::model::change_request_file_data::ChangeRequestFileData;
use crate::domain::model::change_request_file_entry::ChangeRequestFileEntry;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::stream::Stream;

pub(crate) struct CommittedChangeRequest {
    pub commit_id: CommitId,
    pub file_entry: ChangeRequestFileEntry,
}

pub(crate) struct CommittedStreamChange {
    pub stream: Stream,
    pub committed_changes: Vec<CommittedChangeRequestData>,
}

impl CommittedStreamChange {
    pub fn new(stream: Stream, committed_changes: Vec<CommittedChangeRequestData>) -> Self {
        Self {
            stream,
            committed_changes,
        }
    }
}

pub(crate) struct CommittedChangeRequestData {
    pub commit_id: CommitId,
    pub file_data: ChangeRequestFileData,
}
