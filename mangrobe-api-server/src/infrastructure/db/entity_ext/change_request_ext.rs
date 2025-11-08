use crate::domain::model::change_request::{ChangeRequestStatus, ChangeRequestType};
use crate::infrastructure::db::entity::change_requests;
use crate::util::error::MangrobeError;
use anyhow::bail;

pub struct ChangeRequestExt {}

impl ChangeRequestExt {
    pub fn build_domain_status(
        change_request: &change_requests::Model,
    ) -> Result<ChangeRequestStatus, anyhow::Error> {
        match change_request.status {
            0 => Ok(ChangeRequestStatus::New),
            1 => Ok(ChangeRequestStatus::ChangeInserted),
            2 => Ok(ChangeRequestStatus::Committed),
            _ => bail!(MangrobeError::UnexpectedState(
                format!(
                    "invalid status at ChangeRequestStatus: {}",
                    change_request.status
                ),
            )),
        }
    }

    pub fn build_model_status(status: ChangeRequestStatus) -> i32 {
        match status {
            ChangeRequestStatus::New => 0,
            ChangeRequestStatus::ChangeInserted => 1,
            ChangeRequestStatus::Committed => 2,
        }
    }

    pub fn build_model_type(t: ChangeRequestType) -> i32 {
        match t {
            ChangeRequestType::Change => 0,
            ChangeRequestType::Compact => 1,
        }
    }
}
