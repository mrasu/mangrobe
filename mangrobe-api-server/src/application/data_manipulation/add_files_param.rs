use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawAddFileEntry;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::user_table_stream::UserTablStream;

pub struct AddFilesParam {
    pub idempotency_key: IdempotencyKey,
    pub stream: UserTablStream,
    pub entries: Vec<ChangeRequestRawAddFileEntry>,
}
