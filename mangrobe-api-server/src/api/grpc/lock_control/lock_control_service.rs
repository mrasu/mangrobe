use crate::api::core::lock_control::lock_control_service::LockControlService as CoreLockControlService;
use crate::api::grpc::proto::{
    AcquireFileLockRequest, AcquireFileLockResponse, ReleaseFileLockRequest,
    ReleaseFileLockResponse, lock_control_service_server,
};
use crate::api::grpc::util::error::to_grpc_error;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub(crate) struct LockControlService {
    core_service: CoreLockControlService,
}

impl LockControlService {
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            core_service: CoreLockControlService::new(db),
        }
    }
}

#[tonic::async_trait]
impl lock_control_service_server::LockControlService for LockControlService {
    async fn acquire_file_lock(
        &self,
        request: Request<AcquireFileLockRequest>,
    ) -> Result<Response<AcquireFileLockResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .acquire_file_lock(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn release_file_lock(
        &self,
        request: Request<ReleaseFileLockRequest>,
    ) -> Result<Response<ReleaseFileLockResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .release_file_lock(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }
}
