use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawAddFileEntry;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::stream::Stream;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) struct AddFilesParam {
    pub idempotency_key: IdempotencyKey,
    pub table_identifier: TableIdentifier,
    pub stream: Stream,
    pub entries: Vec<ChangeRequestRawAddFileEntry>,
}
