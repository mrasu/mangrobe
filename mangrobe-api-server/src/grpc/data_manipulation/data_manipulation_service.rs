use crate::application::data_manipulation_use_case::DataManipulationUseCase;
use crate::grpc::data_manipulation::change_file_param::ChangeFileParam;
use crate::grpc::proto::{
    ChangeFilesRequest, ChangeFilesResponse, File, GetLatestSnapshotRequest,
    GetLatestSnapshotResponse, Snapshot, data_manipulation_service_server,
};
use crate::grpc::util::error::to_internal_error;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub struct DataManipulationService {
    snapshot_use_case: DataManipulationUseCase,
}

impl DataManipulationService {
    pub fn new(db: &DatabaseConnection) -> Self {
        let snapshot_use_case = DataManipulationUseCase::new(db.clone());
        Self { snapshot_use_case }
    }
}

#[tonic::async_trait]
impl data_manipulation_service_server::DataManipulationService for DataManipulationService {
    async fn get_latest_snapshot(
        &self,
        _req: Request<GetLatestSnapshotRequest>,
    ) -> Result<Response<GetLatestSnapshotResponse>, Status> {
        let snapshot = self
            .snapshot_use_case
            .get_snapshot()
            .await
            .map_err(to_internal_error)?;

        let response = GetLatestSnapshotResponse {
            snapshot: Some(Snapshot {
                files: snapshot
                    .files
                    .iter()
                    .map(|f| File {
                        path: f.path.clone(),
                        size: f.size,
                    })
                    .collect(),
            }),
        };

        Ok(Response::new(response))
    }

    async fn change_files(
        &self,
        request: Request<ChangeFilesRequest>,
    ) -> Result<Response<ChangeFilesResponse>, Status> {
        let req = request.get_ref();
        let param = ChangeFileParam::try_from(req)?;

        let commit_id = self
            .snapshot_use_case
            .change_files(param)
            .await
            .map_err(to_internal_error)?;

        let response = ChangeFilesResponse {
            commit_id: commit_id.into(),
        };
        Ok(Response::new(response))
    }
}
