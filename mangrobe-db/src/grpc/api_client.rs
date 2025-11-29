use crate::grpc::proto::data_manipulation_service_client::DataManipulationServiceClient;
use crate::grpc::proto::lock_control_service_client::LockControlServiceClient;
use crate::grpc::proto::{
    AcquireFileLockRequest, AcquireFileLockResponse, AddFilesRequest, AddFilesResponse,
    ChangeFilesRequest, ChangeFilesResponse, CompactFilesRequest, CompactFilesResponse,
    FileAddEntry, FileCompactDstEntry, FileCompactSrcEntry, FileDeleteEntry, FileLockKey,
    GetCurrentSnapshotRequest, GetCurrentSnapshotResponse, IdempotencyKey, LockFile,
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
        file_add_entries: Vec<FileAddEntry>,
    ) -> Result<Response<AddFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(AddFilesRequest {
            idempotency_key: Some(IdempotencyKey {
                key: Uuid::now_v7().into(),
            }),
            table_id: 0,
            stream_id,
            partition_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            file_add_entries,
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
        file_delete_entries: Vec<FileDeleteEntry>,
    ) -> Result<Response<ChangeFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(ChangeFilesRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
            table_id: 0,
            stream_id,
            partition_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            file_delete_entries,
        });

        self.data_manipulation_service_client
            .change_files(request)
            .await
    }

    pub async fn compact_files(
        &mut self,
        txn_key: Uuid,
        stream_id: i64,
        src_file_entries: Vec<FileCompactSrcEntry>,
        dst_file_entry: FileCompactDstEntry,
    ) -> Result<Response<CompactFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(CompactFilesRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
            table_id: 0,
            stream_id,
            partition_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            src_file_entries,
            dst_file_entry: Some(dst_file_entry),
        });

        self.data_manipulation_service_client
            .compact_files(request)
            .await
    }

    pub async fn acquire_lock(
        &mut self,
        txn_key: Uuid,
        stream_id: i64,
        target_files: Vec<LockFile>,
    ) -> Result<Response<AcquireFileLockResponse>, tonic::Status> {
        let request = tonic::Request::new(AcquireFileLockRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
            ttl_sec: 10,
            table_id: 0,
            stream_id,
            partition_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            target_files,
        });

        self.lock_control_service_client
            .acquire_file_lock(request)
            .await
    }
}
