use crate::grpc::proto::data_manipulation_service_client::DataManipulationServiceClient;
use crate::grpc::proto::lock_control_service_client::LockControlServiceClient;
use crate::grpc::proto::{
    AcquireFileLockEntry, AcquireFileLockRequest, AcquireFileLockResponse, AddFileEntry,
    AddFilesRequest, AddFilesResponse, ChangeFileEntry, ChangeFilesRequest, ChangeFilesResponse,
    CompactFileEntry, CompactFilesRequest, CompactFilesResponse, FileLockKey,
    GetCurrentSnapshotRequest, GetCurrentSnapshotResponse, IdempotencyKey,
};
use prost_types::Timestamp;
use tonic::Response;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Debug)]
pub struct ApiClient {
    data_manipulation_service_client: DataManipulationServiceClient<Channel>,
    lock_control_service_client: LockControlServiceClient<Channel>,
}

impl ApiClient {
    pub fn new(channel: Channel) -> Self {
        let snapshot_service_client = DataManipulationServiceClient::new(channel.clone());
        let lock_service_client = LockControlServiceClient::new(channel.clone());

        Self {
            data_manipulation_service_client: snapshot_service_client,
            lock_control_service_client: lock_service_client,
        }
    }

    pub async fn fetch_snapshot(
        &self,
    ) -> Result<Response<GetCurrentSnapshotResponse>, tonic::Status> {
        let request = tonic::Request::new(GetCurrentSnapshotRequest {
            table_id: 0,
            stream_id: 0,
        });

        self.data_manipulation_service_client
            .clone()
            .get_current_snapshot(request)
            .await
    }

    pub async fn add_files(
        &self,
        stream_id: i64,
        add_file_entries: Vec<AddFileEntry>,
    ) -> Result<Response<AddFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(AddFilesRequest {
            idempotency_key: Some(IdempotencyKey {
                key: Uuid::now_v7().into(),
            }),
            table_id: 0,
            stream_id,
            add_file_entries,
        });

        self.data_manipulation_service_client
            .clone()
            .add_files(request)
            .await
    }

    pub async fn change_files(
        &mut self,
        txn_key: Uuid,
        stream_id: i64,
        change_file_entries: Vec<ChangeFileEntry>,
    ) -> Result<Response<ChangeFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(ChangeFilesRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
            table_id: 0,
            stream_id,
            change_file_entries,
        });

        self.data_manipulation_service_client
            .change_files(request)
            .await
    }

    pub async fn compact_files(
        &mut self,
        txn_key: Uuid,
        stream_id: i64,
        compact_file_entries: Vec<CompactFileEntry>,
    ) -> Result<Response<CompactFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(CompactFilesRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
            table_id: 0,
            stream_id,
            compact_file_entries,
        });

        self.data_manipulation_service_client
            .compact_files(request)
            .await
    }

    pub async fn acquire_lock(
        &mut self,
        txn_key: Uuid,
        stream_id: i64,
        acquire_file_lock_entries: Vec<AcquireFileLockEntry>,
    ) -> Result<Response<AcquireFileLockResponse>, tonic::Status> {
        let request = tonic::Request::new(AcquireFileLockRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
            ttl_sec: 10,
            table_id: 0,
            stream_id,
            acquire_file_lock_entries,
        });

        self.lock_control_service_client
            .acquire_file_lock(request)
            .await
    }
}
