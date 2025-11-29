use crate::application::data_manipulation::add_files_param::AddFilesParam;
use crate::application::data_manipulation::change_files_param::ChangeFilesParam;
use crate::application::data_manipulation::compact_files_param::CompactFilesParam;
use crate::application::data_manipulation::data_manipulation_use_case::DataManipulationUseCase;
use crate::application::data_manipulation::get_current_snapshot_param::GetCurrentSnapshotParam;
use crate::grpc::proto::{
    AddFilesRequest, AddFilesResponse, ChangeFilesRequest, ChangeFilesResponse,
    CompactFilesRequest, CompactFilesResponse, File, GetCurrentSnapshotRequest,
    GetCurrentSnapshotResponse, data_manipulation_service_server,
};
use crate::grpc::util::error::{build_invalid_argument, to_grpc_error};
use chrono::Utc;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

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
        let req = request.get_ref();
        let param = GetCurrentSnapshotParam::try_from(req).map_err(build_invalid_argument)?;

        let snapshot = self
            .data_manipulation_use_case
            .get_current_snapshot(param)
            .await
            .map_err(to_grpc_error)?;

        let response = GetCurrentSnapshotResponse {
            stream_id: snapshot.stream_id.val(),
            commit_id: snapshot.commit_id.map_or_else(|| None, |v| Some(v.val())),
            files: snapshot
                .files
                .iter()
                .map(|f| File {
                    path: f.path.path(),
                    size: f.size,
                })
                .collect(),
        };

        Ok(Response::new(response))
    }

    async fn add_files(
        &self,
        request: Request<AddFilesRequest>,
    ) -> Result<Response<AddFilesResponse>, Status> {
        let req = request.get_ref();
        let param = AddFilesParam::try_from(req).map_err(build_invalid_argument)?;

        let commit_id = self
            .data_manipulation_use_case
            .add_files(param)
            .await
            .map_err(to_grpc_error)?;

        let response = AddFilesResponse {
            commit_id: commit_id.into(),
        };
        Ok(Response::new(response))
    }

    async fn change_files(
        &self,
        request: Request<ChangeFilesRequest>,
    ) -> Result<Response<ChangeFilesResponse>, Status> {
        let request_started_at = Utc::now();
        let req = request.get_ref();
        let param =
            ChangeFilesParam::new(req, request_started_at).map_err(build_invalid_argument)?;

        let commit_id = self
            .data_manipulation_use_case
            .change_files(param)
            .await
            .map_err(to_grpc_error)?;

        let response = ChangeFilesResponse {
            commit_id: commit_id.into(),
        };
        Ok(Response::new(response))
    }

    async fn compact_files(
        &self,
        request: Request<CompactFilesRequest>,
    ) -> Result<Response<CompactFilesResponse>, Status> {
        let request_started_at = Utc::now();

        let req = request.get_ref();
        let param =
            CompactFilesParam::new(req, request_started_at).map_err(build_invalid_argument)?;

        let commit_id = self
            .data_manipulation_use_case
            .compact_files(param)
            .await
            .map_err(to_grpc_error)?;

        let response = CompactFilesResponse {
            commit_id: commit_id.into(),
        };
        Ok(Response::new(response))
    }
}
