use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
#[serde(transparent)]
pub struct StreamId(i64);

impl From<StreamId> for i64 {
    fn from(id: StreamId) -> Self {
        id.0
    }
}

impl From<i64> for StreamId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

impl From<&i64> for StreamId {
    fn from(id: &i64) -> Self {
        Self(*id)
    }
}

impl StreamId {
    pub fn val(&self) -> i64 {
        self.0
    }
}
