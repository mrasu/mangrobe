use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawChangeFilesEntry;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::user_table_stream::UserTablStream;

pub struct ChangeFilesParam {
    pub file_lock_key: FileLockKey,
    pub stream: UserTablStream,
    pub entries: Vec<ChangeRequestRawChangeFilesEntry>,
}
