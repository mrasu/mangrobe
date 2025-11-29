use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawCompactFilesEntry;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use chrono::{DateTime, Utc};

pub struct CompactFilesParam {
    pub file_lock_key: FileLockKey,
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,
    pub entry: ChangeRequestRawCompactFilesEntry,
}
