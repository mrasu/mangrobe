use crate::domain::model::change_request_file_entry::ChangeRequestFileEntry;
use crate::domain::model::file_id::FileId;

// bag of changes to be committed. Holding ids to be used directly.
#[derive(Default)]
pub struct Changeset {
    pub add_file_ids: Vec<FileId>,
    pub delete_file_ids: Vec<FileId>,
}

impl Changeset {
    pub fn new_from_change_file_entries(entry: &ChangeRequestFileEntry) -> Self {
        let mut add_file_ids = vec![];
        let mut delete_file_ids = vec![];

        match entry {
            ChangeRequestFileEntry::AddFiles { add_files } => {
                add_file_ids.extend(add_files.file_ids.clone())
            }
            ChangeRequestFileEntry::ChangeFiles { add_files } => {
                add_file_ids.extend(add_files.file_ids.clone())
            }
            ChangeRequestFileEntry::Compact { compact } => {
                add_file_ids.push(compact.dst_file_id.clone());
                delete_file_ids.extend(compact.src_file_ids.clone());
            }
        }

        Self {
            add_file_ids,
            delete_file_ids,
        }
    }
}
