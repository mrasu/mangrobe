pub struct ChangeLogId {
    val: i64,
}

impl ChangeLogId {}

impl From<ChangeLogId> for i64 {
    fn from(id: ChangeLogId) -> Self {
        id.val
    }
}

impl From<i64> for ChangeLogId {
    fn from(id: i64) -> Self {
        Self { val: id }
    }
}
