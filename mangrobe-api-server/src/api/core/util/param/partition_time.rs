use crate::api::core::util::error::ParameterError;
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
