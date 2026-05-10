use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawCompactFilesEntry;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) struct CompactFilesParam {
    pub file_lock_key: FileLockKey,
    pub table_identifier: TableIdentifier,
    pub stream_id: StreamId,
    pub entries: Vec<ChangeRequestRawCompactFilesEntry>,
}
