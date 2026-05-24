use crate::domain::model::partition::{Partition, PartitionError, UnvalidatedPartition};
use crate::domain::model::table_definition::PartitionDataType;

#[derive(Clone, Debug)]
pub(crate) struct PartitionFilter {
    pub data_type: PartitionDataType,
    pub predicates: Vec<PartitionPredicate>,
}

impl PartitionFilter {
    pub fn new(data_type: PartitionDataType, predicates: Vec<PartitionPredicate>) -> Self {
        Self {
            data_type,
            predicates,
        }
    }

    pub fn should_filter(&self) -> bool {
        !self.predicates.is_empty()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PartitionPredicate {
    In(PartitionIn),
    Range(PartitionRange),
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionIn {
    pub partitions: Vec<Partition>,
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionRange {
    pub lower: Option<PartitionBound>,
    pub upper: Option<PartitionBound>,
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionBound {
    pub partition: Partition,
    pub inclusivity: BoundInclusivity,
}

#[derive(Clone, Debug)]
pub(crate) enum BoundInclusivity {
    Inclusive,
    Exclusive,
}

#[derive(Clone, Debug)]
pub(crate) struct UnvalidatedPartitionFilter {
    pub predicates: Vec<UnvalidatedPartitionPredicate>,
}

impl UnvalidatedPartitionFilter {
    pub fn new(predicates: Vec<UnvalidatedPartitionPredicate>) -> Self {
        Self { predicates }
    }

    pub fn validate(
        &self,
        partition_data_type: &PartitionDataType,
    ) -> Result<PartitionFilter, PartitionError> {
        let validated_predicates = self
            .predicates
            .iter()
            .map(|v| v.validate(partition_data_type))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(PartitionFilter::new(
            partition_data_type.clone(),
            validated_predicates,
        ))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum UnvalidatedPartitionPredicate {
    In(UnvalidatedPartitionIn),
    Range(UnvalidatedPartitionRange),
}

impl UnvalidatedPartitionPredicate {
    pub fn validate(
        &self,
        partition_data_type: &PartitionDataType,
    ) -> Result<PartitionPredicate, PartitionError> {
        match self {
            UnvalidatedPartitionPredicate::In(v) => {
                Ok(PartitionPredicate::In(v.validate(partition_data_type)?))
            }
            UnvalidatedPartitionPredicate::Range(v) => {
                Ok(PartitionPredicate::Range(v.validate(partition_data_type)?))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct UnvalidatedPartitionIn {
    pub partitions: Vec<UnvalidatedPartition>,
}

impl UnvalidatedPartitionIn {
    pub fn validate(
        &self,
        partition_data_type: &PartitionDataType,
    ) -> Result<PartitionIn, PartitionError> {
        let partitions = self
            .partitions
            .iter()
            .map(|v| v.validate(partition_data_type))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(PartitionIn { partitions })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct UnvalidatedPartitionRange {
    pub lower: Option<UnvalidatedPartitionBound>,
    pub upper: Option<UnvalidatedPartitionBound>,
}

impl UnvalidatedPartitionRange {
    pub fn validate(
        &self,
        partition_data_type: &PartitionDataType,
    ) -> Result<PartitionRange, PartitionError> {
        Ok(PartitionRange {
            lower: self
                .lower
                .as_ref()
                .map(|v| v.validate(partition_data_type))
                .transpose()?,
            upper: self
                .upper
                .as_ref()
                .map(|v| v.validate(partition_data_type))
                .transpose()?,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct UnvalidatedPartitionBound {
    pub partition: UnvalidatedPartition,
    pub inclusivity: BoundInclusivity,
}

impl UnvalidatedPartitionBound {
    pub fn validate(
        &self,
        partition_data_type: &PartitionDataType,
    ) -> Result<PartitionBound, PartitionError> {
        Ok(PartitionBound {
            partition: self.partition.validate(partition_data_type)?,
            inclusivity: self.inclusivity.clone(),
        })
    }
}
