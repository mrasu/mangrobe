use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::grpc::proto::FileLockKey as FileLockKeyParam;
use crate::grpc::proto::IdempotencyKey as IdempotencyKeyParam;
use crate::util::error::ParameterError;
use chrono::{DateTime, Utc};
use prost_types::Timestamp;

pub fn to_partition_time(param: Option<Timestamp>) -> Result<DateTime<Utc>, ParameterError> {
    let req_partition_time = param.ok_or(ParameterError::Required("partition_time".into()))?;

    DateTime::from_timestamp(req_partition_time.seconds, req_partition_time.nanos as u32).ok_or(
        ParameterError::Invalid(
            "partition_time".to_string(),
            "out-of-range number of seconds or nanos".to_string(),
        ),
    )
}

pub fn to_idempotency_key(
    param: Option<IdempotencyKeyParam>,
) -> Result<IdempotencyKey, ParameterError> {
    let Some(param) = param else {
        return Err(ParameterError::Required("idempotency_key".to_string()));
    };

    IdempotencyKey::try_from(param.key)
        .map_err(|msg| ParameterError::Invalid("idempotency_key".to_string(), msg))
}

pub fn to_file_lock_key(
    param: Option<FileLockKeyParam>,
    request_started_at: DateTime<Utc>,
) -> Result<FileLockKey, ParameterError> {
    let Some(param) = param else {
        return Err(ParameterError::Required("file_lock_key".to_string()));
    };

    FileLockKey::new(param.key, request_started_at)
        .map_err(|msg| ParameterError::Invalid("file_lock_key".to_string(), msg))
}
