use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
#[serde(transparent)]
pub struct FileId(i64);

impl From<FileId> for i64 {
    fn from(id: FileId) -> Self {
        id.0
    }
}

impl From<i64> for FileId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

impl FileId {
    pub fn val(&self) -> i64 {
        self.0
    }
}
