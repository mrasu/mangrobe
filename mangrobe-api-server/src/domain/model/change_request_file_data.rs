use crate::domain::model::file::FilePath;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ChangeRequestFileData {
    AddFiles {
        add_files: ChangeRequestAddFilesData,
    },
    ChangeFiles {
        change_files: ChangeRequestChangeFilesData,
    },
    Compact {
        compact: ChangeRequestCompactFilesData,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChangeRequestAddFilesData {
    pub files: Vec<FileData>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChangeRequestChangeFilesData {
    pub delete_files: Vec<FileData>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChangeRequestCompactFilesData {
    pub compacted_files: Vec<ChangeRequestCompactFileData>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChangeRequestCompactFileData {
    pub src_files: Vec<FileData>,
    pub dst_file: FileData,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FileData {
    pub path: FilePath,
}

impl FileData {
    pub fn new(path: FilePath) -> Self {
        Self { path }
    }
}
