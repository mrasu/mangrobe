use crate::domain::model::change_request_file_entry::{
    ChangeRequestChangeFilesEntry, ChangeRequestCompactFilesEntry,
};
use crate::domain::model::file_id::FileId;

// bag of changes to be committed. Holding ids to be used directly.
#[derive(Default)]
pub struct Changeset {
    pub add_file_ids: Vec<FileId>,
    pub delete_file_ids: Vec<FileId>,
}

impl Changeset {
    pub fn new_from_change_file_entries(entry: ChangeRequestChangeFilesEntry) -> Self {
        Self {
            add_file_ids: vec![],
            delete_file_ids: entry.delete_file_ids,
        }
    }

    pub fn new_from_compact_file_entries(entry: ChangeRequestCompactFilesEntry) -> Self {
        Self {
            add_file_ids: entry
                .entries
                .iter()
                .map(|e| e.dst_file_id.clone())
                .collect(),
            delete_file_ids: entry
                .entries
                .iter()
                .flat_map(|e| e.src_file_ids.clone())
                .collect(),
        }
    }
}
