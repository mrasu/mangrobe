
pub struct CommitId {
    val: i64,
}

impl CommitId {}

impl From<CommitId> for i64 {
    fn from(id: CommitId) -> Self {
        id.val
    }
}

impl From<i64> for CommitId {
    fn from(id: i64) -> Self {
        Self { val: id }
    }
}

impl From<&i64> for CommitId {
    fn from(id: &i64) -> Self {
        Self { val: *id }
    }
}

impl CommitId {
    pub fn val(&self) -> i64 {
        self.val
    }
}
