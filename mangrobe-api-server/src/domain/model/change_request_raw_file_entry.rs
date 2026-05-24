use crate::domain::model::file::{FileEntry, FilePath};
use crate::domain::model::partition::UnvalidatedPartition;

#[derive(Debug)]
pub(crate) struct ChangeRequestRawAddFileEntry {
    pub partition: UnvalidatedPartition,
    pub files_to_add: Vec<FileEntry>,
}

impl ChangeRequestRawAddFileEntry {
    pub fn new(partition: UnvalidatedPartition, files_to_add: Vec<FileEntry>) -> Self {
        Self {
            partition,
            files_to_add,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ChangeRequestRawChangeFilesEntry {
    pub partition: UnvalidatedPartition,
    pub files_to_delete: Vec<FilePath>,
}

impl ChangeRequestRawChangeFilesEntry {
    pub fn new(partition: UnvalidatedPartition, files_to_delete: Vec<FilePath>) -> Self {
        Self {
            partition,
            files_to_delete,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ChangeRequestRawCompactFilesEntry {
    pub partition: UnvalidatedPartition,
    pub info_entries: Vec<ChangeRequestRawCompactFileInfoEntry>,
}

impl ChangeRequestRawCompactFilesEntry {
    pub fn new(
        partition: UnvalidatedPartition,
        info_entries: Vec<ChangeRequestRawCompactFileInfoEntry>,
    ) -> Self {
        Self {
            partition,
            info_entries,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ChangeRequestRawCompactFileInfoEntry {
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
