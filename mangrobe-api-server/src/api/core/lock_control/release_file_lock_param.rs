use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::file_lock_key::to_file_lock_key;
use crate::api::grpc::proto::ReleaseFileLockRequest;
use crate::domain::model::file_lock_key::FileLockKey;
use chrono::{DateTime, Utc};

pub(crate) fn build_release_file_lock_param(
    req: &ReleaseFileLockRequest,
    request_started_at: DateTime<Utc>,
) -> Result<FileLockKey, ParameterError> {
    to_file_lock_key(req.file_lock_key.clone(), request_started_at)
}
