use crate::domain::model::change_request_file_entry::{
    ChangeRequestAddFilesEntry, ChangeRequestChangeFilesEntry, ChangeRequestCompactFilesEntry,
    ChangeRequestFileEntry,
};
use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::changeset::Changeset;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use chrono::{DateTime, Utc};
use std::cmp::PartialEq;
use strum_macros::Display;

pub trait ChangeRequestTrait {
    fn id(&self) -> &ChangeRequestId;
    fn set_status(&self, status: ChangeRequestStatus) -> Self;
}

#[derive(Clone)]
pub struct BaseChangeRequest {
    pub id: ChangeRequestId,

    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,

    pub status: ChangeRequestStatus,

    pub change_type: ChangeRequestType,
}

impl ChangeRequestTrait for BaseChangeRequest {
    fn id(&self) -> &ChangeRequestId {
        &self.id
    }

    fn set_status(&self, status: ChangeRequestStatus) -> Self {
        let mut cloned = self.clone();

        cloned.status = status;
        cloned
    }
}

#[derive(Clone)]
pub struct ChangeRequest {
    pub base: BaseChangeRequest,

    pub file_entry: Option<ChangeRequestFileEntry>,
}

impl ChangeRequestTrait for ChangeRequest {
    fn id(&self) -> &ChangeRequestId {
        &self.base.id
    }

    fn set_status(&self, status: ChangeRequestStatus) -> Self {
        let mut cloned = self.clone();

        cloned.base.status = status;
        cloned
    }
}

#[derive(Clone)]
pub struct ChangeRequestForAdd {
    pub base: BaseChangeRequest,
    pub change_files_entry: ChangeRequestAddFilesEntry,
}

impl ChangeRequestTrait for ChangeRequestForAdd {
    fn id(&self) -> &ChangeRequestId {
        &self.base.id
    }

    fn set_status(&self, status: ChangeRequestStatus) -> Self {
        let mut cloned = self.clone();

        cloned.base.status = status;
        cloned
    }
}

impl ChangeRequestForAdd {
    pub fn new(base: BaseChangeRequest, add_files_entry: ChangeRequestAddFilesEntry) -> Self {
        Self {
            base,
            change_files_entry: add_files_entry,
        }
    }
}

#[derive(Clone)]
pub struct ChangeRequestForChange {
    pub base: BaseChangeRequest,
    pub change_files_entry: ChangeRequestChangeFilesEntry,
}

impl ChangeRequestTrait for ChangeRequestForChange {
    fn id(&self) -> &ChangeRequestId {
        &self.base.id
    }

    fn set_status(&self, status: ChangeRequestStatus) -> Self {
        let mut cloned = self.clone();

        cloned.base.status = status;
        cloned
    }
}

impl ChangeRequestForChange {
    pub fn new(base: BaseChangeRequest, change_files_entry: ChangeRequestChangeFilesEntry) -> Self {
        Self {
            base,
            change_files_entry,
        }
    }

    pub fn to_changeset(&self) -> Changeset {
        Changeset::new_from_change_file_entries(self.change_files_entry.clone())
    }
}

#[derive(Clone)]
pub struct ChangeRequestForCompact {
    pub base: BaseChangeRequest,
    pub compact_files_entry: ChangeRequestCompactFilesEntry,
}

impl ChangeRequestForCompact {
    pub fn new(
        base: BaseChangeRequest,
        compact_files_entry: ChangeRequestCompactFilesEntry,
    ) -> Self {
        Self {
            base,
            compact_files_entry,
        }
    }

    pub fn to_changeset(&self) -> Changeset {
        Changeset::new_from_compact_file_entries(self.compact_files_entry.clone())
    }
}

impl ChangeRequestTrait for ChangeRequestForCompact {
    fn id(&self) -> &ChangeRequestId {
        &self.base.id
    }

    fn set_status(&self, status: ChangeRequestStatus) -> Self {
        let mut cloned = self.clone();

        cloned.base.status = status;
        cloned
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
