use crate::domain::model::file_id::FileId;

#[derive(Clone, Debug)]
pub(crate) struct CurrentFile {
    pub file_id: FileId,
}

impl CurrentFile {
    pub fn new(file_id: FileId) -> Self {
        Self { file_id }
    }
}
