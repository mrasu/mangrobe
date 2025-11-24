use crate::domain::model::change_request::{ChangeRequestStatus, ChangeRequestType};
use crate::domain::model::change_request_file_entry::ChangeRequestFileEntry;
use crate::infrastructure::db::entity::change_requests;
use crate::util::error::MangrobeError;
use anyhow::bail;
use serde_json::Value;

pub struct ChangeRequestExt {}

impl ChangeRequestExt {
    pub fn build_domain_status(
        change_request: &change_requests::Model,
    ) -> Result<ChangeRequestStatus, anyhow::Error> {
        match change_request.status {
            0 => Ok(ChangeRequestStatus::New),
            1 => Ok(ChangeRequestStatus::ChangeInserted),
            2 => Ok(ChangeRequestStatus::Committed),
            _ => bail!(MangrobeError::UnexpectedState(format!(
                "invalid status at ChangeRequestStatus: {}",
                change_request.status
            ),)),
        }
    }

    pub fn build_model_status(status: ChangeRequestStatus) -> i32 {
        match status {
            ChangeRequestStatus::New => 0,
            ChangeRequestStatus::ChangeInserted => 1,
            ChangeRequestStatus::Committed => 2,
        }
    }

    pub fn build_domain_change_type(
        change_request: &change_requests::Model,
    ) -> Result<ChangeRequestType, anyhow::Error> {
        match change_request.change_type {
            0 => Ok(ChangeRequestType::AddFiles),
            1 => Ok(ChangeRequestType::Change),
            2 => Ok(ChangeRequestType::Compact),
            _ => bail!(MangrobeError::UnexpectedState(format!(
                "invalid status at ChangeRequestStatus: {}",
                change_request.status
            ),)),
        }
    }

    pub fn build_model_change_type(t: ChangeRequestType) -> i32 {
        match t {
            ChangeRequestType::AddFiles => 0,
            ChangeRequestType::Change => 1,
            ChangeRequestType::Compact => 2,
        }
    }

    pub fn build_domain_file_entry(
        change_request: &change_requests::Model,
    ) -> Result<Option<ChangeRequestFileEntry>, anyhow::Error> {
        match &change_request.file_entry {
            Some(json) => Ok(serde_json::from_value(json.clone())?),
            None => Ok(None),
        }
    }

    pub fn build_model_file_entry(
        change_file_entry: &ChangeRequestFileEntry,
    ) -> Result<Value, serde_json::Error> {
        serde_json::to_value(change_file_entry)
    }
}
