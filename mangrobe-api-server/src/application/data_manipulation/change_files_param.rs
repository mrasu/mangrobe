use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawChangeFilesEntry;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::stream::Stream;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) struct ChangeFilesParam {
    pub file_lock_key: FileLockKey,
    pub table_identifier: TableIdentifier,
    pub stream: Stream,
    pub entries: Vec<ChangeRequestRawChangeFilesEntry>,
}
