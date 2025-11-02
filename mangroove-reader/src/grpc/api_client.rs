use crate::grpc::proto::snapshot_service_client::SnapshotServiceClient;
use crate::grpc::proto::{GetSnapshotRequest, GetSnapshotResponse, Snapshot};
use tonic::Response;
use tonic::transport::Channel;

#[derive(Debug)]
pub struct ApiClient {
    snapshot_service_client: SnapshotServiceClient<Channel>,
}

impl ApiClient {
    pub fn new(channel: Channel) -> Self {
        let snapshot_service_client = SnapshotServiceClient::new(channel);

        Self {
            snapshot_service_client,
        }
    }

    pub async fn fetch_snapshot(&self) -> Result<Response<GetSnapshotResponse>, tonic::Status> {
        let request = tonic::Request::new(GetSnapshotRequest {});
        let response = self
            .snapshot_service_client
            .clone()
            .get_snapshot(request)
            .await?;

        Ok(response)
    }
}
