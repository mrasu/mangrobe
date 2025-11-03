use crate::application::snapshot::SnapshotUseCase;
use crate::grpc::proto::{
    File, GetSnapshotRequest, GetSnapshotResponse, Snapshot, snapshot_service_server,
};
use crate::grpc::util::to_internal_error;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub struct SnapshotService {
    snapshot_use_case: SnapshotUseCase,
}

impl SnapshotService {
    pub fn new(db: &DatabaseConnection) -> Self {
        let snapshot_use_case = SnapshotUseCase::new(db.clone());
        Self { snapshot_use_case }
    }
}

#[tonic::async_trait]
impl snapshot_service_server::SnapshotService for SnapshotService {
    async fn get_snapshot(
        &self,
        _req: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        let snapshot = self
            .snapshot_use_case
            .get_snapshot()
            .await
            .map_err(to_internal_error)?;

        let response = GetSnapshotResponse {
            snapshot: Some(Snapshot {
                // files: vec![
                //     File {
                //         name: "example1.vortex".to_string(),
                //         size: 12572,
                //     },
                //     File {
                //         name: "example2.vortex".to_string(),
                //         size: 31388,
                //     },
                // ],
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
}
