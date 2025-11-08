use crate::domain::model::file::File;
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct ChangeRequestCompactFileEntry {
    pub tenant_id: i64,
    pub partition_time: DateTime<Utc>,
    pub src_entries: Vec<FilePath>,
    pub dst_entry: ChangeRequestFileCompactDstEntry,
}

impl ChangeRequestCompactFileEntry {
    pub fn new(
        tenant_id: i64,
        partition_time: DateTime<Utc>,
        src_entries: Vec<FilePath>,
        dst_entry: ChangeRequestFileCompactDstEntry,
    ) -> Self {
        Self {
            tenant_id,
            partition_time,
            src_entries,
            dst_entry,
        }
    }

    pub fn dst_file(&self) -> File {
        File {
            tenant_id: self.tenant_id,
            partition_time: self.partition_time,
            path: self.dst_entry.path.clone(),
            size: self.dst_entry.size,
        }
    }
}

#[derive(Debug)]
pub struct FilePath {
    pub path: String,
}

impl FilePath {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

#[derive(Debug)]
pub struct ChangeRequestFileCompactDstEntry {
    pub path: String,
    pub size: i64,
}

impl ChangeRequestFileCompactDstEntry {
    pub fn new(path: String, size: i64) -> Self {
        Self { path, size }
    }
}
