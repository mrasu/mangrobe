use crate::api::core::util::error::ParameterError;
use crate::domain::model::file_lock_key::FileLockKey;
use chrono::{DateTime, Utc};

pub fn to_file_lock_key(
    param: Option<crate::api::grpc::proto::FileLockKey>,
    request_started_at: DateTime<Utc>,
) -> Result<FileLockKey, ParameterError> {
    let Some(param) = param else {
        return Err(ParameterError::Required("file_lock_key".to_string()));
    };

    FileLockKey::new(param.key, request_started_at)
        .map_err(|msg| ParameterError::Invalid("file_lock_key".to_string(), msg))
}
