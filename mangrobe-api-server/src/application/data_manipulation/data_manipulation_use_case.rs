use crate::application::data_manipulation::change_files_param::ChangeFilesParam;
use crate::application::data_manipulation::compact_files_param::CompactFilesParam;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::snapshot::Snapshot;
use crate::domain::service::change_request_service::ChangeRequestService;
use crate::domain::service::snapshot_service::SnapshotService;
use sea_orm::DatabaseConnection;

pub struct DataManipulationUseCase {
    snapshot_service: SnapshotService,
    change_request_service: ChangeRequestService,
}

impl DataManipulationUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            snapshot_service: SnapshotService::new(&connection),
            change_request_service: ChangeRequestService::new(&connection),
        }
    }

    pub async fn get_snapshot(&self) -> Result<Snapshot, anyhow::Error> {
        self.snapshot_service.get_latest().await
    }

    pub async fn change_files(&self, param: ChangeFilesParam) -> Result<CommitId, anyhow::Error> {
        let change_request = self
            .change_request_service
            .find_or_create(param.idempotency_key, param.tenant_id, param.partition_time)
            .await?;

        self.change_request_service
            .insert_entries(&change_request, &param.entries)
            .await?;

        self.change_request_service
            .commit_change_request(&change_request)
            .await
    }

    pub async fn compact_files(&self, param: CompactFilesParam) -> Result<CommitId, anyhow::Error> {
        let change_request = self
            .change_request_service
            .find_or_create(param.idempotency_key, param.tenant_id, param.partition_time)
            .await?;

        self.change_request_service
            .insert_compaction_entry(&change_request, &param.entry)
            .await?;

        self.change_request_service
            .commit_change_request(&change_request)
            .await
    }
}
