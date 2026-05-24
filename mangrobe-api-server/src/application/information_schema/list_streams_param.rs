use crate::domain::model::stream::Stream;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) struct ListStreamsParam {
    pub table_identifier: TableIdentifier,
    pub stream_after: Option<Stream>,
}
