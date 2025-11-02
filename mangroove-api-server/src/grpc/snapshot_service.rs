use crate::grpc::proto::{
    File, GetSnapshotRequest, GetSnapshotResponse, Snapshot, snapshot_service_server,
};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct SnapshotService;

#[tonic::async_trait]
impl snapshot_service_server::SnapshotService for SnapshotService {
    async fn get_snapshot(
        &self,
        _req: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        let response = GetSnapshotResponse {
            snapshot: Some(Snapshot {
                files: vec![
                    File {
                        name: "example1.vortex".to_string(),
                        size: 12572,
                    },
                    File {
                        name: "example2.vortex".to_string(),
                        size: 31388,
                    },
                ],
            }),
        };

        Ok(Response::new(response))
    }
}
