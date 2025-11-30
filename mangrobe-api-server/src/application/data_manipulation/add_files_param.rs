use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawAddFileEntry;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;

pub struct AddFilesParam {
    pub idempotency_key: IdempotencyKey,
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub entries: Vec<ChangeRequestRawAddFileEntry>,
}
