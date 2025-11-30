use crate::domain::model::file_id::FileId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChangeRequestAddFilesEntry {
    pub file_ids: Vec<FileId>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChangeRequestChangeFilesEntry {
    pub delete_file_ids: Vec<FileId>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChangeRequestCompactFilesEntry {
    pub entries: Vec<ChangeRequestCompactFileEntry>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChangeRequestCompactFileEntry {
    pub src_file_ids: Vec<FileId>,
    pub dst_file_id: FileId,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ChangeRequestFileEntry {
    AddFiles {
        add_files: ChangeRequestAddFilesEntry,
    },
    ChangeFiles {
        change_files: ChangeRequestChangeFilesEntry,
    },
    Compact {
        compact: ChangeRequestCompactFilesEntry,
    },
}
