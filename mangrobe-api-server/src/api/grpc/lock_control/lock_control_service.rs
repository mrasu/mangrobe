use crate::api::core::lock_control::acquire_file_lock_response::build_acquire_file_lock_response;
use crate::api::core::lock_control::release_file_lock_param::build_release_file_lock_param;
use crate::api::core::lock_control::release_file_lock_response::build_release_file_lock_response;
use crate::api::grpc::lock_control::acquire_file_lock_param::build_acquire_file_lock_param;
use crate::api::grpc::proto::{
    AcquireFileLockRequest, AcquireFileLockResponse, ReleaseFileLockRequest,
    ReleaseFileLockResponse, lock_control_service_server,
};
use crate::api::grpc::util::error::{build_invalid_argument, to_grpc_error};
use crate::application::lock_control::lock_control_use_case::LockControlUseCase;
use chrono::Utc;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub(crate) struct LockControlService {
    lock_control_use_case: LockControlUseCase,
}

impl LockControlService {
    pub fn new(db: &DatabaseConnection) -> Self {
        Self {
            lock_control_use_case: LockControlUseCase::new(db.clone()),
        }
    }
}

#[tonic::async_trait]
impl lock_control_service_server::LockControlService for LockControlService {
    async fn acquire_file_lock(
        &self,
        request: Request<AcquireFileLockRequest>,
    ) -> Result<Response<AcquireFileLockResponse>, Status> {
        let request_started_at = Utc::now();
        let param = build_acquire_file_lock_param(request, request_started_at)
            .map_err(build_invalid_argument)?;

        let locked_files = self
            .lock_control_use_case
            .acquire_lock(param)
            .await
            .map_err(to_grpc_error)?;

        let response = build_acquire_file_lock_response(&locked_files);

        Ok(Response::new(response))
    }

    async fn release_file_lock(
        &self,
        request: Request<ReleaseFileLockRequest>,
    ) -> Result<Response<ReleaseFileLockResponse>, Status> {
        let request_started_at = Utc::now();
        let file_lock_key = build_release_file_lock_param(request.get_ref(), request_started_at)
            .map_err(build_invalid_argument)?;

        let deleted = self
            .lock_control_use_case
            .release_lock(file_lock_key)
            .await
            .map_err(to_grpc_error)?;

        let response = build_release_file_lock_response(deleted);
        Ok(Response::new(response))
    }
}
