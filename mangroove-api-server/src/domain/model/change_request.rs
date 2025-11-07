use chrono::{DateTime, Utc};
use strum_macros::Display;

pub struct ChangeRequest {
    pub id: i64,
    pub idempotency_key: Vec<u8>,

    pub tenant_id: i64,
    pub partition_time: DateTime<Utc>,

    pub status: ChangeRequestStatus,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Display)]
pub enum ChangeRequestStatus {
    New = 0,
    ChangeInserted = 1,
    Committed = 2,
}

impl ChangeRequestStatus {
    pub fn is_completed(&self, target: ChangeRequestStatus) -> bool {
        *self >= target
    }

    pub fn can_progress_to(&self, target: ChangeRequestStatus) -> bool {
        (*self as i32) + 1 == target as i32
    }
}
