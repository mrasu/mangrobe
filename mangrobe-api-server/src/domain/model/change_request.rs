use crate::domain::model::change_request_file_entry::ChangeRequestFileEntry;
use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::changeset::Changeset;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use chrono::{DateTime, Utc};
use std::cmp::PartialEq;
use strum_macros::Display;

#[derive(Clone)]
pub struct ChangeRequest {
    pub id: ChangeRequestId,
    pub idempotency_key: IdempotencyKey,

    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,

    pub status: ChangeRequestStatus,

    pub change_type: ChangeRequestType,
    pub file_entry: Option<ChangeRequestFileEntry>,
}

pub trait ChangeRequestAbs {
    fn id(&self) -> &ChangeRequestId;
    fn idempotency_key(&self) -> &IdempotencyKey;
    fn set_status(&self, status: ChangeRequestStatus) -> Self;
}

impl ChangeRequestAbs for ChangeRequest {
    fn id(&self) -> &ChangeRequestId {
        &self.id
    }

    fn idempotency_key(&self) -> &IdempotencyKey {
        &self.idempotency_key
    }

    fn set_status(&self, status: ChangeRequestStatus) -> Self {
        let mut cloned = self.clone();

        cloned.status = status;
        cloned
    }
}

impl ChangeRequest {
    pub fn unwrap_to_with_file_entry(&self) -> ChangeRequestWithFileEntry {
        let file_entry = self.file_entry.clone().unwrap();
        self.with_file_entry(file_entry)
    }

    pub fn with_file_entry(
        &self,
        file_entry: ChangeRequestFileEntry,
    ) -> ChangeRequestWithFileEntry {
        ChangeRequestWithFileEntry {
            id: self.id.clone(),
            idempotency_key: self.idempotency_key.clone(),
            user_table_id: self.user_table_id.clone(),
            stream_id: self.stream_id.clone(),
            partition_time: self.partition_time,
            status: self.status,
            change_type: self.change_type.clone(),
            file_entry,
        }
    }
}

#[derive(Clone)]
pub struct ChangeRequestWithFileEntry {
    pub id: ChangeRequestId,
    pub idempotency_key: IdempotencyKey,

    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,

    pub status: ChangeRequestStatus,

    pub change_type: ChangeRequestType,
    pub file_entry: ChangeRequestFileEntry,
}

impl ChangeRequestAbs for ChangeRequestWithFileEntry {
    fn id(&self) -> &ChangeRequestId {
        &self.id
    }

    fn idempotency_key(&self) -> &IdempotencyKey {
        &self.idempotency_key
    }

    fn set_status(&self, status: ChangeRequestStatus) -> Self {
        let mut cloned = self.clone();

        cloned.status = status;
        cloned
    }
}

impl ChangeRequestWithFileEntry {
    pub fn to_changeset(&self) -> Changeset {
        Changeset::new_from_change_file_entries(&self.file_entry)
    }
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

#[derive(Clone, PartialEq)]
pub enum ChangeRequestType {
    AddFiles,
    Change,
    Compact,
}
