use crate::domain::model::file::FilePath;
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct LockFileRawAcquireEntry {
    pub partition_time: DateTime<Utc>,
    pub file_paths: Vec<FilePath>,
}

impl LockFileRawAcquireEntry {
    pub fn new(partition_time: DateTime<Utc>, file_paths: Vec<FilePath>) -> Self {
        Self {
            partition_time,
            file_paths,
        }
    }
}
