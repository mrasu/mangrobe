use crate::domain::model::stream_id::StreamId;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) struct ListStreamsParam {
    pub table_identifier: TableIdentifier,
    pub stream_id_after: Option<StreamId>,
}
