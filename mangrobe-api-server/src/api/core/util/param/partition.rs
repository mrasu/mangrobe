use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::PartitionValue as ProtoPartitionValue;
use crate::domain::model::partition::{Partition, UnvalidatedPartition};
use crate::domain::model::table_definition::PartitionDataType;
use crate::proto::partition_value::Value;
use chrono::DateTime;
use prost_types::Timestamp;

pub(crate) fn to_unvalidated_partition(
    param: Option<&ProtoPartitionValue>,
) -> Result<UnvalidatedPartition, ParameterError> {
    let value = param
        .and_then(|param| param.value.as_ref())
        .ok_or(ParameterError::Required("partition".into()))?;

    match value {
        Value::Int64Value(value) => Ok(UnvalidatedPartition::Int64(*value)),
        Value::Time(time) => {
            let Some(chrono_time) = DateTime::from_timestamp(time.seconds, time.nanos as u32)
            else {
                return Err(ParameterError::Invalid(
                    "partition".into(),
                    "cannot parse time".to_string(),
                ));
            };
            Ok(UnvalidatedPartition::Time(chrono_time))
        }
    }
}

pub(crate) fn to_proto_partition_value(
    partition_data_type: &PartitionDataType,
    partition: Partition,
) -> ProtoPartitionValue {
    ProtoPartitionValue {
        value: match partition_data_type {
            PartitionDataType::TimeMicrosecond => {
                Some(Value::Time(micros_to_timestamp(&partition.val())))
            }
            PartitionDataType::Int64 => Some(Value::Int64Value(partition.val())),
        },
    }
}

fn micros_to_timestamp(micros: &i64) -> Timestamp {
    let seconds = micros / 1_000_000;
    let nanos = ((micros % 1_000_000) * 1_000) as i32;

    Timestamp { seconds, nanos }
}
