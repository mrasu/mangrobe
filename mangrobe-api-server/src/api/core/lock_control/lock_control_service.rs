use crate::api::core::lock_control::acquire_file_lock_param::build_acquire_file_lock_param;
use crate::api::core::lock_control::acquire_file_lock_response::build_acquire_file_lock_response;
use crate::api::core::lock_control::release_file_lock_param::build_release_file_lock_param;
use crate::api::core::lock_control::release_file_lock_response::build_release_file_lock_response;
use crate::api::grpc::proto::{
    AcquireFileLockRequest, AcquireFileLockResponse, ReleaseFileLockRequest,
    ReleaseFileLockResponse,
};
use crate::application::lock_control::lock_control_use_case::LockControlUseCase;
use chrono::Utc;
use sea_orm::DatabaseConnection;

pub struct LockControlService {
    lock_control_use_case: LockControlUseCase,
}

impl LockControlService {
    pub(crate) fn new(db: DatabaseConnection) -> Self {
        Self {
            lock_control_use_case: LockControlUseCase::new(db),
        }
    }

    pub async fn acquire_file_lock(
        &self,
        param: AcquireFileLockRequest,
    ) -> Result<AcquireFileLockResponse, anyhow::Error> {
        let request_started_at = Utc::now();
        let param = build_acquire_file_lock_param(&param, request_started_at)?;

        let locked_files = self.lock_control_use_case.acquire_lock(param).await?;

        Ok(build_acquire_file_lock_response(&locked_files))
    }

    pub async fn release_file_lock(
        &self,
        param: ReleaseFileLockRequest,
    ) -> Result<ReleaseFileLockResponse, anyhow::Error> {
        let request_started_at = Utc::now();
        let file_lock_key = build_release_file_lock_param(&param, request_started_at)?;

        let deleted = self
            .lock_control_use_case
            .release_lock(file_lock_key)
            .await?;

        Ok(build_release_file_lock_response(deleted))
    }
}
