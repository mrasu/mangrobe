use ahash::{HashSet, HashSetExt};
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

    pub fn has_same(a: &[FileId], b: &[FileId]) -> bool {
        if a.len() != b.len() {
            return false;
        }

        let mut map: HashSet<i64> = HashSet::new();
        for id in a {
            map.insert(id.val());
        }

        for id in b {
            if !map.contains(&id.val()) {
                return false;
            }
        }

        true
    }
}
