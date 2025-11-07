use crate::domain::model::change_request::ChangeRequestStatus;
use crate::infrastructure::db::entity::change_requests;
use crate::util::error::MangobeError;
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
            _ => bail!(MangobeError::UnexpectedState(
                format!(
                    "invalid status at ChangeRequestStatus: {}",
                    change_request.status
                )
                .into(),
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
}
