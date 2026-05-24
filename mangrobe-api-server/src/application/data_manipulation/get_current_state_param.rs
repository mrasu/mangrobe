use crate::domain::model::partition_filter::UnvalidatedPartitionFilter;
use crate::domain::model::stream::Stream;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) struct GetCurrentStateParam {
    pub table_identifier: TableIdentifier,
    pub stream: Stream,
    pub unvalidated_partition_filter: UnvalidatedPartitionFilter,
}
