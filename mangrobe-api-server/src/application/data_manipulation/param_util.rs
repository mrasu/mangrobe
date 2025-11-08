use crate::domain::model::idempotency_key::IdempotencyKey;
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

pub fn to_idempotency_key(param: Vec<u8>) -> Result<IdempotencyKey, ParameterError> {
    IdempotencyKey::try_from(param)
        .map_err(|msg| ParameterError::Invalid("idempotency_key".to_string(), msg))
}
