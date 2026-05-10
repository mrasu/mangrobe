use crate::domain::model::partition_time_filter::PartitionTimeFilter;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) struct GetCurrentStateParam {
    pub table_identifier: TableIdentifier,
    pub stream_id: StreamId,
    pub partition_time_filter: PartitionTimeFilter,
}
