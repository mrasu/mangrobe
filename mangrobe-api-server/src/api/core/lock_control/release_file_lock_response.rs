use crate::api::grpc::proto::ReleaseFileLockResponse;

pub(crate) fn build_release_file_lock_response(deleted: bool) -> ReleaseFileLockResponse {
    ReleaseFileLockResponse { deleted }
}
