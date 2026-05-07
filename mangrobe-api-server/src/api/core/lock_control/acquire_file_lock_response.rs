use crate::api::grpc::proto::{AcquireFileLockResponse, File};
use crate::domain::model::file::FileWithId;

pub(crate) fn build_acquire_file_lock_response(
    locked_files: &[FileWithId],
) -> AcquireFileLockResponse {
    AcquireFileLockResponse {
        files: locked_files
            .iter()
            .map(|f| File {
                file_id: f.id.val().to_string(),
                path: f.file.path.path(),
                size: f.file.size,
            })
            .collect(),
    }
}
