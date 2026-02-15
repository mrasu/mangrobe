use crate::domain::model::file::{File, FileWithId};
use crate::domain::model::file_column_statistics::FileColumnStatistics;
use crate::domain::model::file_id::FileId;
use crate::domain::model::file_metadata::FileMetadata;

#[derive(Clone, Debug)]
pub struct FileWithStatistics {
    pub id: FileId,
    pub file: File,
    pub column_statistics: Vec<FileColumnStatistics>,
    pub file_metadata: Option<FileMetadata>,
}

impl FileWithStatistics {
    pub fn from_models(
        file: FileWithId,
        column_statistics: Vec<FileColumnStatistics>,
        file_metadata: Option<FileMetadata>,
    ) -> Self {
        Self {
            id: file.id,
            file: file.file,
            column_statistics,
            file_metadata,
        }
    }
}
