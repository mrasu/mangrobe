use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
#[serde(transparent)]
pub(crate) struct Stream(i64);

impl From<Stream> for i64 {
    fn from(stream: Stream) -> Self {
        stream.0
    }
}

impl From<i64> for Stream {
    fn from(stream: i64) -> Self {
        Self(stream)
    }
}

impl From<&i64> for Stream {
    fn from(stream: &i64) -> Self {
        Self(*stream)
    }
}

impl Stream {
    pub fn val(&self) -> i64 {
        self.0
    }
}
