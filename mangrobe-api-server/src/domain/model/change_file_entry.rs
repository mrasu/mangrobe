use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::file_id::FileId;
use sea_orm::Set;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize, Debug)]
pub struct ChangeFileAddEntry {
    pub file_id: FileId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChangeFileCompactEntry {
    pub src_file_ids: Vec<FileId>,
    pub dst_file_id: FileId,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ChangeFileEntry {
    Add { add: ChangeFileAddEntry },
    Compact { compact: ChangeFileCompactEntry },
}

impl ChangeFileEntry {
    pub fn pick_active_file_ids(
        ordered_change_request_ids: &Vec<ChangeRequestId>,
        change_file_entries: HashMap<ChangeRequestId, Vec<ChangeFileEntry>>,
    ) -> Vec<FileId> {
        let mut file_ids = HashSet::<FileId>::new();
        for change_request_id in ordered_change_request_ids {
            let Some(entries) = change_file_entries.get(change_request_id) else {
                continue;
            };

            for entry in entries {
                match entry {
                    ChangeFileEntry::Add { add } => {
                        file_ids.insert(add.file_id.clone());
                    }
                    ChangeFileEntry::Compact { compact } => {
                        compact
                            .src_file_ids
                            .iter()
                            .for_each(|f| _ = file_ids.remove(&f));

                        file_ids.insert(compact.dst_file_id.clone());
                    }
                }
            }
        }

        file_ids.into_iter().collect()
    }
}
