#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ChangeRequestId {
    val: i64,
}

impl ChangeRequestId {
    pub fn val(&self) -> i64 {
        self.val
    }
}

impl From<&ChangeRequestId> for i64 {
    fn from(id: &ChangeRequestId) -> Self {
        id.val
    }
}

impl From<ChangeRequestId> for i64 {
    fn from(id: ChangeRequestId) -> Self {
        id.val
    }
}

impl From<i64> for ChangeRequestId {
    fn from(id: i64) -> Self {
        Self { val: id }
    }
}
