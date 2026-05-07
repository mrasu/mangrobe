use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub(crate) struct PartitionTimeFilter {
    pub predicates: Vec<PartitionTimePredicate>,
}

impl PartitionTimeFilter {
    pub fn new(predicates: Vec<PartitionTimePredicate>) -> Self {
        Self { predicates }
    }

    pub fn should_filter(&self) -> bool {
        !self.predicates.is_empty()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PartitionTimePredicate {
    In(PartitionTimeIn),
    Range(PartitionTimeRange),
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionTimeIn {
    pub times: Vec<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionTimeRange {
    pub lower: Option<PartitionTimeBound>,
    pub upper: Option<PartitionTimeBound>,
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionTimeBound {
    pub time: DateTime<Utc>,
    pub inclusivity: BoundInclusivity,
}

#[derive(Clone, Debug)]
pub(crate) enum BoundInclusivity {
    Inclusive,
    Exclusive,
}
