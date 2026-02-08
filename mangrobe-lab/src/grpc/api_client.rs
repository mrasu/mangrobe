use crate::grpc::proto::data_manipulation_service_client::DataManipulationServiceClient;
use crate::grpc::proto::lock_control_service_client::LockControlServiceClient;
use crate::grpc::proto::{
    AcquireFileLockEntry, AcquireFileLockRequest, AcquireFileLockResponse, AddFileEntry,
    AddFilesRequest, AddFilesResponse, ChangeFileEntry, ChangeFilesRequest, ChangeFilesResponse,
    CompactFileEntry, CompactFilesRequest, CompactFilesResponse, FileLockKey,
    GetCurrentStateRequest, GetCurrentStateResponse, IdempotencyKey, ReleaseFileLockRequest,
    ReleaseFileLockResponse,
};
use crate::proto::data_definition_service_client::DataDefinitionServiceClient;
use crate::proto::{CreateTableRequest, CreateTableResponse};
use tonic::Response;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ApiClient {
    data_manipulation_service_client: DataManipulationServiceClient<Channel>,
    data_definition_service_client: DataDefinitionServiceClient<Channel>,
    lock_control_service_client: LockControlServiceClient<Channel>,
}

impl ApiClient {
    pub fn new(channel: Channel) -> Self {
        let data_manipulation_service_client = DataManipulationServiceClient::new(channel.clone());
        let data_definition_service_client = DataDefinitionServiceClient::new(channel.clone());
        let lock_control_service_client = LockControlServiceClient::new(channel.clone());

        Self {
            data_manipulation_service_client,
            data_definition_service_client,
            lock_control_service_client,
        }
    }

    pub async fn create_table(
        &self,
        table_name: String,
        skip_if_exists: bool,
    ) -> Result<Response<CreateTableResponse>, tonic::Status> {
        let request = tonic::Request::new(CreateTableRequest {
            table_name,
            skip_if_exists,
        });

        self.data_definition_service_client
            .clone()
            .create_table(request)
            .await
    }

    pub async fn fetch_current_state(
        &self,
        table_name: String,
        stream_id: i64,
    ) -> Result<Response<GetCurrentStateResponse>, tonic::Status> {
        let request = tonic::Request::new(GetCurrentStateRequest {
            table_name,
            stream_id,
        });

        self.data_manipulation_service_client
            .clone()
            .get_current_state(request)
            .await
    }

    pub async fn add_files(
        &self,
        table_name: String,
        stream_id: i64,
        add_file_entries: Vec<AddFileEntry>,
    ) -> Result<Response<AddFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(AddFilesRequest {
            idempotency_key: Some(IdempotencyKey {
                key: Uuid::now_v7().into(),
            }),
            table_name,
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
        table_name: String,
        stream_id: i64,
        change_file_entries: Vec<ChangeFileEntry>,
    ) -> Result<Response<ChangeFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(ChangeFilesRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
            table_name,
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
        table_name: String,
        stream_id: i64,
        compact_file_entries: Vec<CompactFileEntry>,
    ) -> Result<Response<CompactFilesResponse>, tonic::Status> {
        let request = tonic::Request::new(CompactFilesRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
            table_name,
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
        table_name: String,
        stream_id: i64,
        acquire_file_lock_entries: Vec<AcquireFileLockEntry>,
    ) -> Result<Response<AcquireFileLockResponse>, tonic::Status> {
        let request = tonic::Request::new(AcquireFileLockRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
            ttl_sec: 10,
            table_name,
            stream_id,
            acquire_file_lock_entries,
        });

        self.lock_control_service_client
            .acquire_file_lock(request)
            .await
    }

    pub async fn release_lock(
        &mut self,
        txn_key: Uuid,
    ) -> Result<Response<ReleaseFileLockResponse>, tonic::Status> {
        let request = tonic::Request::new(ReleaseFileLockRequest {
            file_lock_key: Some(FileLockKey {
                key: txn_key.into(),
            }),
        });

        self.lock_control_service_client
            .release_file_lock(request)
            .await
    }
}
