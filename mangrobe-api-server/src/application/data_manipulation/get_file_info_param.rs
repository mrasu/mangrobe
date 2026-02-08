use crate::domain::model::file_id::FileId;

#[derive(Debug, Clone)]
pub struct FileStatisticsSelection {
    pub includes_min: bool,
    pub includes_max: bool,
}

impl FileStatisticsSelection {
    pub fn new(includes_min: bool, includes_max: bool) -> Self {
        Self {
            includes_min,
            includes_max,
        }
    }
}

#[derive(Clone)]
pub struct GetFileInfoParam {
    pub file_ids: Vec<FileId>,
    pub stat_types: FileStatisticsSelection,
}
