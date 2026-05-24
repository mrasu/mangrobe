use crate::domain::model::file::FilePath;
use crate::domain::model::partition::UnvalidatedPartition;

#[derive(Debug)]
pub(crate) struct LockFileRawAcquireEntry {
    pub partition: UnvalidatedPartition,
    pub file_paths: Vec<FilePath>,
}

impl LockFileRawAcquireEntry {
    pub fn new(partition: UnvalidatedPartition, file_paths: Vec<FilePath>) -> Self {
        Self {
            partition,
            file_paths,
        }
    }
}
