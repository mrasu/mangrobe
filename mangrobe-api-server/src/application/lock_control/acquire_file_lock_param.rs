use crate::domain::model::file::FilePath;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use chrono::{DateTime, Duration, Utc};

pub struct AcquireFileLockParam {
    pub file_lock_key: FileLockKey,
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,
    pub ttl: Duration,
    pub paths: Vec<FilePath>,
}
