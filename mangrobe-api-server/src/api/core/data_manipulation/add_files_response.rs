use crate::api::grpc::proto::AddFilesResponse;
use crate::domain::model::commit_id::CommitId;

pub(crate) fn build_add_files_response(commit_id: CommitId) -> AddFilesResponse {
    AddFilesResponse {
        commit_id: commit_id.to_string(),
    }
}
