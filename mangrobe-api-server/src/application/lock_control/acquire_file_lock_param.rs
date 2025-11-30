use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::lock_raw_file_entry::LockFileRawAcquireEntry;
use crate::domain::model::user_table_stream::UserTablStream;
use chrono::Duration;

pub struct AcquireFileLockParam {
    pub file_lock_key: FileLockKey,
    pub stream: UserTablStream,
    pub ttl: Duration,
    pub entries: Vec<LockFileRawAcquireEntry>,
}
