use crate::api::grpc::proto::ChangeFilesResponse;
use crate::domain::model::commit_id::CommitId;

pub(crate) fn build_change_files_response(commit_id: CommitId) -> ChangeFilesResponse {
    ChangeFilesResponse {
        commit_id: commit_id.to_string(),
    }
}
