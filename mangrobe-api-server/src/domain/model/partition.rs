use crate::domain::model::partition::PartitionError::IncorrectDataType;
use crate::domain::model::table_definition::PartitionDataType;
use chrono::{DateTime, Utc};
use std::fmt;
use thiserror::Error;

#[derive(Ord, Clone, Debug, Eq, PartialOrd, PartialEq)]
pub(crate) struct Partition(i64);

impl Partition {
    // External values must be validated before constructing a Partition.
    // DB values are assumed to be valid.
    pub fn build_from_validated(v: i64) -> Self {
        Self(v)
    }

    pub fn val(&self) -> i64 {
        self.0
    }
}

#[derive(Clone, Debug)]
pub(crate) enum UnvalidatedPartition {
    Int64(i64),
    Time(DateTime<Utc>),
}

impl UnvalidatedPartition {
    pub fn validate(&self, data_type: &PartitionDataType) -> Result<Partition, PartitionError> {
        match (self, data_type) {
            (UnvalidatedPartition::Int64(v), PartitionDataType::Int64) => {
                Ok(Partition::build_from_validated(*v))
            }
            (UnvalidatedPartition::Time(v), PartitionDataType::TimeMicrosecond) => {
                Ok(Partition::build_from_validated(v.timestamp_micros()))
            }
            _ => Err(IncorrectDataType(data_type.clone(), self.clone())),
        }
    }
}

impl fmt::Display for UnvalidatedPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnvalidatedPartition::Int64(v) => write!(f, "int64({v})"),
            UnvalidatedPartition::Time(v) => write!(f, "time({})", v.to_rfc3339()),
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum PartitionError {
    #[error("Incorrect data type. expected {0} but {1}")]
    IncorrectDataType(PartitionDataType, UnvalidatedPartition),
}
