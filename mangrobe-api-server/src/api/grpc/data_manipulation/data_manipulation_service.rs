use crate::api::core::data_manipulation::data_manipulation_service::DataManipulationService as CoreDataManipulationService;
use crate::api::grpc::proto::{
    AddFilesRequest, AddFilesResponse, ChangeFilesRequest, ChangeFilesResponse,
    CompactFilesRequest, CompactFilesResponse, GetCommitsRequest, GetCommitsResponse,
    GetCurrentStateRequest, GetCurrentStateResponse, GetFileInfoRequest, GetFileInfoResponse,
    data_manipulation_service_server,
};
use crate::api::grpc::util::error::to_grpc_error;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub(crate) struct DataManipulationService {
    core_service: CoreDataManipulationService,
}

impl DataManipulationService {
    pub fn new(db: &DatabaseConnection) -> Self {
        Self {
            core_service: CoreDataManipulationService::new(db.clone()),
        }
    }
}

#[tonic::async_trait]
impl data_manipulation_service_server::DataManipulationService for DataManipulationService {
    async fn get_current_state(
        &self,
        request: Request<GetCurrentStateRequest>,
    ) -> Result<Response<GetCurrentStateResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .get_current_state(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn get_commits(
        &self,
        request: Request<GetCommitsRequest>,
    ) -> Result<Response<GetCommitsResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .get_commits(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn get_file_info(
        &self,
        request: Request<GetFileInfoRequest>,
    ) -> Result<Response<GetFileInfoResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .get_file_info(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn add_files(
        &self,
        request: Request<AddFilesRequest>,
    ) -> Result<Response<AddFilesResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .add_files(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn change_files(
        &self,
        request: Request<ChangeFilesRequest>,
    ) -> Result<Response<ChangeFilesResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .change_files(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn compact_files(
        &self,
        request: Request<CompactFilesRequest>,
    ) -> Result<Response<CompactFilesResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .compact_files(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }
}
