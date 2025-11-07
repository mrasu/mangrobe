use crate::grpc::proto::action_service_client::ActionServiceClient;
use crate::grpc::proto::snapshot_service_client::SnapshotServiceClient;
use crate::grpc::proto::{
    ChangeFilesRequest, ChangeFilesResponse, FileAddEntry, GetSnapshotRequest, GetSnapshotResponse,
};
use tonic::Response;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Debug)]
pub struct ApiClient {
    snapshot_service_client: SnapshotServiceClient<Channel>,
    action_service_client: ActionServiceClient<Channel>,
}

impl ApiClient {
    pub fn new(channel: Channel) -> Self {
        let snapshot_service_client = SnapshotServiceClient::new(channel.clone());
        let action_service_client = ActionServiceClient::new(channel.clone());

        Self {
            snapshot_service_client,
            action_service_client,
        }
    }

    pub async fn fetch_snapshot(&self) -> Result<Response<GetSnapshotResponse>, tonic::Status> {
        let request = tonic::Request::new(GetSnapshotRequest {});

        self.snapshot_service_client
            .clone()
            .get_snapshot(request)
            .await
    }

    pub async fn add_files(
        &self,
        file_add_entries: Vec<FileAddEntry>,
    ) -> Result<Response<ChangeFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(ChangeFilesRequest {
            idempotency_key: Uuid::now_v7().into(),
            tenant_id: 0,
            partition_time: prost_types::Timestamp::default().into(),
            file_add_entries,
        });

        self.action_service_client
            .clone()
            .change_files(request)
            .await
    }
}
