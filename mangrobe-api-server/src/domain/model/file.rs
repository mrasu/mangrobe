use crate::domain::model::file_column_statistics::FileColumnStatistics;
use crate::domain::model::file_id::FileId;
use crate::domain::model::file_metadata::FileMetadata;
use crate::domain::model::user_table_stream::UserTablStream;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_128;

#[derive(Clone, Debug)]
pub struct File {
    pub stream: UserTablStream,
    pub partition_time: DateTime<Utc>,
    pub path: FilePath,
    pub size: i64,
}

impl File {
    pub fn new(
        stream: UserTablStream,
        partition_time: DateTime<Utc>,
        path: FilePath,
        size: i64,
    ) -> Self {
        Self {
            stream,
            partition_time,
            path,
            size,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileWithId {
    pub id: FileId,
    pub file: File,
}

impl FileWithId {
    pub fn new(
        id: FileId,
        stream: UserTablStream,
        partition_time: DateTime<Utc>,
        path: FilePath,
        size: i64,
    ) -> Self {
        Self {
            id,
            file: File::new(stream, partition_time, path, size),
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
    pub column_statistics: Vec<FileColumnStatistics>,
    pub file_metadata: Option<FileMetadata>,
}

impl FileEntry {
    pub fn new(
        path: FilePath,
        size: i64,
        column_statistics: Vec<FileColumnStatistics>,
        file_metadata: Option<FileMetadata>,
    ) -> Self {
        Self {
            path,
            size,
            column_statistics,
            file_metadata,
        }
    }

    pub fn to_file(&self, stream: UserTablStream, partition_time: DateTime<Utc>) -> File {
        File::new(stream, partition_time, self.path.clone(), self.size)
    }
}
