use crate::application::data_manipulation::data_manipulation_use_case::DataManipulationUseCase;
use crate::grpc::data_manipulation::add_files_param::build_add_files_param;
use crate::grpc::data_manipulation::change_files_param::build_change_file_param;
use crate::grpc::data_manipulation::compact_files_param::build_compact_files_param;
use crate::grpc::data_manipulation::get_changes_param::build_get_commits_param;
use crate::grpc::data_manipulation::get_changes_response::build_get_commits_response;
use crate::grpc::data_manipulation::get_current_snapshot_param::build_get_current_snapshot_param;
use crate::grpc::proto::{
    AddFilesRequest, AddFilesResponse, ChangeFilesRequest, ChangeFilesResponse,
    CompactFilesRequest, CompactFilesResponse, File, GetCommitsRequest, GetCommitsResponse,
    GetCurrentSnapshotRequest, GetCurrentSnapshotResponse, data_manipulation_service_server,
};
use crate::grpc::util::error::{build_invalid_argument, to_grpc_error};
use chrono::Utc;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

const CHANGES_LIMIT_PER_STREAM: u64 = 100;

pub struct DataManipulationService {
    data_manipulation_use_case: DataManipulationUseCase,
}

impl DataManipulationService {
    pub fn new(db: &DatabaseConnection) -> Self {
        let snapshot_use_case = DataManipulationUseCase::new(db.clone());
        Self {
            data_manipulation_use_case: snapshot_use_case,
        }
    }
}

#[tonic::async_trait]
impl data_manipulation_service_server::DataManipulationService for DataManipulationService {
    async fn get_current_snapshot(
        &self,
        request: Request<GetCurrentSnapshotRequest>,
    ) -> Result<Response<GetCurrentSnapshotResponse>, Status> {
        let param = build_get_current_snapshot_param(request).map_err(build_invalid_argument)?;

        let snapshot = self
            .data_manipulation_use_case
            .get_current_snapshot(param)
            .await
            .map_err(to_grpc_error)?;

        let response = GetCurrentSnapshotResponse {
            commit_id: snapshot
                .commit_id
                .map_or_else(|| None, |v| Some(v.to_string())),
            files: snapshot
                .files
                .iter()
                .map(|f| File {
                    path: f.file.path.path(),
                    size: f.file.size,
                })
                .collect(),
        };

        Ok(Response::new(response))
    }

    async fn get_commits(
        &self,
        request: Request<GetCommitsRequest>,
    ) -> Result<Response<GetCommitsResponse>, Status> {
        let param = build_get_commits_param(request).map_err(build_invalid_argument)?;

        let changes = self
            .data_manipulation_use_case
            .get_changes(&param, CHANGES_LIMIT_PER_STREAM)
            .await
            .map_err(to_grpc_error)?;

        let response = build_get_commits_response(&param.table_id, changes);
        Ok(Response::new(response))
    }

    async fn add_files(
        &self,
        request: Request<AddFilesRequest>,
    ) -> Result<Response<AddFilesResponse>, Status> {
        let param = build_add_files_param(request).map_err(build_invalid_argument)?;

        let commit_id = self
            .data_manipulation_use_case
            .add_files(param)
            .await
            .map_err(to_grpc_error)?;

        let response = AddFilesResponse {
            commit_id: commit_id.to_string(),
        };
        Ok(Response::new(response))
    }

    async fn change_files(
        &self,
        request: Request<ChangeFilesRequest>,
    ) -> Result<Response<ChangeFilesResponse>, Status> {
        let request_started_at = Utc::now();
        let param =
            build_change_file_param(request, request_started_at).map_err(build_invalid_argument)?;

        let commit_id = self
            .data_manipulation_use_case
            .change_files(param)
            .await
            .map_err(to_grpc_error)?;

        let response = ChangeFilesResponse {
            commit_id: commit_id.to_string(),
        };
        Ok(Response::new(response))
    }

    async fn compact_files(
        &self,
        request: Request<CompactFilesRequest>,
    ) -> Result<Response<CompactFilesResponse>, Status> {
        let request_started_at = Utc::now();
        let param = build_compact_files_param(request, request_started_at)
            .map_err(build_invalid_argument)?;

        let commit_id = self
            .data_manipulation_use_case
            .compact_files(param)
            .await
            .map_err(to_grpc_error)?;

        let response = CompactFilesResponse {
            commit_id: commit_id.to_string(),
        };
        Ok(Response::new(response))
    }
}
