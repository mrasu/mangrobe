use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_128;

#[derive(Clone, Debug)]
pub struct File {
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,
    pub path: FilePath,
    pub size: i64,
}

impl File {
    pub fn new(
        user_table_id: UserTableId,
        stream_id: StreamId,
        partition_time: DateTime<Utc>,
        path: FilePath,
        size: i64,
    ) -> Self {
        Self {
            user_table_id,
            stream_id,
            partition_time,
            path,
            size,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FilePath {
    path: String,
}

impl FilePath {
    fn new(path: String) -> Self {
        Self { path }
    }

    pub fn path(&self) -> String {
        self.path.clone()
    }

    pub fn to_xxh3_128(&self) -> Vec<u8> {
        xxh3_128(self.path.as_bytes()).to_be_bytes().to_vec()
    }
}

impl From<String> for FilePath {
    fn from(path: String) -> FilePath {
        FilePath::new(path)
    }
}

#[derive(Debug)]
pub struct FileEntry {
    pub path: FilePath,
    pub size: i64,
}

impl FileEntry {
    pub fn new(path: FilePath, size: i64) -> Self {
        Self { path, size }
    }

    pub fn to_file(
        &self,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        partition_time: DateTime<Utc>,
    ) -> File {
        File::new(
            user_table_id.clone(),
            stream_id.clone(),
            partition_time,
            self.path.clone(),
            self.size,
        )
    }
}
