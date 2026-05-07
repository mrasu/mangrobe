use crate::api::grpc::proto::CompactFilesResponse;
use crate::domain::model::commit_id::CommitId;

pub(crate) fn build_compact_files_response(commit_id: CommitId) -> CompactFilesResponse {
    CompactFilesResponse {
        commit_id: commit_id.to_string(),
    }
}
