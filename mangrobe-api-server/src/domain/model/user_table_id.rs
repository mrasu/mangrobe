use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
#[serde(transparent)]
pub struct UserTableId(i64);

impl From<UserTableId> for i64 {
    fn from(id: UserTableId) -> Self {
        id.0
    }
}

impl From<i64> for UserTableId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

impl UserTableId {
    pub fn val(&self) -> i64 {
        self.0
    }
}
