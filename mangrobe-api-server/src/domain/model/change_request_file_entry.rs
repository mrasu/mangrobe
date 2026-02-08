use crate::domain::model::change_request_file_data::{
    ChangeRequestAddFilesData, ChangeRequestChangeFilesData, ChangeRequestCompactFileData,
    ChangeRequestCompactFilesData, ChangeRequestFileData, FileData,
};
use crate::domain::model::file::FileWithId;
use crate::domain::model::file_id::FileId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

impl ChangeRequestFileEntry {
    pub fn file_ids(&self) -> Vec<FileId> {
        match self {
            ChangeRequestFileEntry::AddFiles { add_files } => add_files.file_ids.clone(),
            ChangeRequestFileEntry::ChangeFiles { change_files } => {
                change_files.delete_file_ids.clone()
            }
            ChangeRequestFileEntry::Compact { compact } => compact
                .entries
                .iter()
                .flat_map(|entry| entry.src_file_ids.iter().chain([&entry.dst_file_id]))
                .cloned()
                .collect(),
        }
    }

    pub fn to_file_data(&self, file_map: &HashMap<FileId, FileWithId>) -> ChangeRequestFileData {
        match self {
            ChangeRequestFileEntry::AddFiles { add_files } => ChangeRequestFileData::AddFiles {
                add_files: ChangeRequestAddFilesData {
                    files: self.build_file_datas(file_map, &add_files.file_ids),
                },
            },
            ChangeRequestFileEntry::ChangeFiles { change_files } => {
                ChangeRequestFileData::ChangeFiles {
                    change_files: ChangeRequestChangeFilesData {
                        delete_files: self
                            .build_file_datas(file_map, &change_files.delete_file_ids),
                    },
                }
            }
            ChangeRequestFileEntry::Compact { compact } => ChangeRequestFileData::Compact {
                compact: ChangeRequestCompactFilesData {
                    compacted_files: compact
                        .entries
                        .iter()
                        .map(|entry| ChangeRequestCompactFileData {
                            src_files: self.build_file_datas(file_map, &entry.src_file_ids),
                            dst_file: self.build_file_data(file_map, &entry.dst_file_id),
                        })
                        .collect(),
                },
            },
        }
    }

    fn build_file_datas(
        &self,
        file_map: &HashMap<FileId, FileWithId>,
        file_ids: &[FileId],
    ) -> Vec<FileData> {
        file_ids
            .iter()
            .map(|id| self.build_file_data(file_map, id))
            .collect()
    }

    fn build_file_data(
        &self,
        file_map: &HashMap<FileId, FileWithId>,
        file_id: &FileId,
    ) -> FileData {
        FileData::new(file_id.clone(), file_map[file_id].file.path.clone())
    }
}
