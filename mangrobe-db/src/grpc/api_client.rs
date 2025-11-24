use crate::grpc::proto::data_manipulation_service_client::DataManipulationServiceClient;
use crate::grpc::proto::{
    ChangeFilesRequest, ChangeFilesResponse, FileAddEntry, GetCurrentSnapshotRequest,
    GetCurrentSnapshotResponse,
};
use tonic::Response;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Debug)]
pub struct ApiClient {
    snapshot_service_client: DataManipulationServiceClient<Channel>,
}

impl ApiClient {
    pub fn new(channel: Channel) -> Self {
        let snapshot_service_client = DataManipulationServiceClient::new(channel.clone());

        Self {
            snapshot_service_client,
        }
    }

    pub async fn fetch_snapshot(
        &self,
    ) -> Result<Response<GetCurrentSnapshotResponse>, tonic::Status> {
        let request = tonic::Request::new(GetCurrentSnapshotRequest {
            table_id: 0,
            stream_id: 0,
        });

        self.snapshot_service_client
            .clone()
            .get_current_snapshot(request)
            .await
    }

    pub async fn add_files(
        &self,
        file_add_entries: Vec<FileAddEntry>,
    ) -> Result<Response<ChangeFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(ChangeFilesRequest {
            idempotency_key: Uuid::now_v7().into(),
            table_id: 0,
            stream_id: 0,
            partition_time: prost_types::Timestamp::default().into(),
            file_add_entries,
        });

        self.snapshot_service_client
            .clone()
            .change_files(request)
            .await
    }
}
