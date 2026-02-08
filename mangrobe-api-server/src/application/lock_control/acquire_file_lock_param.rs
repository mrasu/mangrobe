use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::lock_raw_file_entry::LockFileRawAcquireEntry;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_name::UserTableName;
use chrono::Duration;

pub struct AcquireFileLockParam {
    pub file_lock_key: FileLockKey,
    pub table_name: UserTableName,
    pub stream_id: StreamId,
    pub ttl: Duration,
    pub entries: Vec<LockFileRawAcquireEntry>,
}
