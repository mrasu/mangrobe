use crate::domain::model::file::{FileEntry, FilePath};

#[derive(Debug)]
pub struct ChangeRequestRawAddFilesEntry {
    pub files: Vec<FileEntry>,
}

impl ChangeRequestRawAddFilesEntry {
    pub fn new(files: Vec<FileEntry>) -> Self {
        Self { files }
    }
}

#[derive(Debug)]
pub struct ChangeRequestRawChangeFilesEntry {
    pub files_to_delete: Vec<FilePath>,
}

impl ChangeRequestRawChangeFilesEntry {
    pub fn new(files_to_delete: Vec<FilePath>) -> Self {
        Self { files_to_delete }
    }
}

#[derive(Debug)]
pub struct ChangeRequestRawCompactFilesEntry {
    pub src_file_paths: Vec<FilePath>,
    pub dst_file: FileEntry,
}

impl ChangeRequestRawCompactFilesEntry {
    pub fn new(src_file_paths: Vec<FilePath>, dst_file: FileEntry) -> Self {
        Self {
            src_file_paths,
            dst_file,
        }
    }
}
