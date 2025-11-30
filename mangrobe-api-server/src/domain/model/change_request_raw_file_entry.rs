use crate::domain::model::file::{FileEntry, FilePath};
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct ChangeRequestRawAddFileEntry {
    pub partition_time: DateTime<Utc>,
    pub files_to_add: Vec<FileEntry>,
}

impl ChangeRequestRawAddFileEntry {
    pub fn new(partition_time: DateTime<Utc>, files_to_add: Vec<FileEntry>) -> Self {
        Self {
            partition_time,
            files_to_add,
        }
    }
}

#[derive(Debug)]
pub struct ChangeRequestRawChangeFilesEntry {
    pub partition_time: DateTime<Utc>,
    pub files_to_delete: Vec<FilePath>,
}

impl ChangeRequestRawChangeFilesEntry {
    pub fn new(partition_time: DateTime<Utc>, files_to_delete: Vec<FilePath>) -> Self {
        Self {
            partition_time,
            files_to_delete,
        }
    }
}

#[derive(Debug)]
pub struct ChangeRequestRawCompactFilesEntry {
    pub partition_time: DateTime<Utc>,
    pub info_entries: Vec<ChangeRequestRawCompactFileInfoEntry>,
}

impl ChangeRequestRawCompactFilesEntry {
    pub fn new(
        partition_time: DateTime<Utc>,
        info_entries: Vec<ChangeRequestRawCompactFileInfoEntry>,
    ) -> Self {
        Self {
            partition_time,
            info_entries,
        }
    }
}

#[derive(Debug)]
pub struct ChangeRequestRawCompactFileInfoEntry {
    pub src_file_paths: Vec<FilePath>,
    pub dst_file: FileEntry,
}

impl ChangeRequestRawCompactFileInfoEntry {
    pub fn new(src_file_paths: Vec<FilePath>, dst_file: FileEntry) -> Self {
        Self {
            src_file_paths,
            dst_file,
        }
    }
}
