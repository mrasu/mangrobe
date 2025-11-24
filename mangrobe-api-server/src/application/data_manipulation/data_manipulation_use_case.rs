use crate::application::data_manipulation::add_files_param::AddFilesParam;
use crate::application::data_manipulation::change_files_param::ChangeFilesParam;
use crate::application::data_manipulation::compact_files_param::CompactFilesParam;
use crate::application::data_manipulation::get_current_snapshot_param::GetCurrentSnapshotParam;
use crate::domain::model::change_request::ChangeRequestType;
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

    pub async fn get_current_snapshot(
        &self,
        param: GetCurrentSnapshotParam,
    ) -> Result<Snapshot, anyhow::Error> {
        self.snapshot_service
            .get_current(&param.user_table_id, &param.stream_id)
            .await
    }

    pub async fn add_files(&self, param: AddFilesParam) -> Result<CommitId, anyhow::Error> {
        let change_request = self
            .change_request_service
            .find_or_create(
                &param.idempotency_key,
                &param.user_table_id,
                &param.stream_id,
                &param.partition_time,
                ChangeRequestType::AddFiles,
            )
            .await?;

        let change_request_with_entry = self
            .change_request_service
            .apply_add_entry(&change_request, &param.entry)
            .await?;

        self.change_request_service
            .commit_change_request(&change_request_with_entry)
            .await
    }

    pub async fn change_files(&self, param: ChangeFilesParam) -> Result<CommitId, anyhow::Error> {
        let change_request = self
            .change_request_service
            .find_or_create(
                &param.idempotency_key,
                &param.user_table_id,
                &param.stream_id,
                &param.partition_time,
                ChangeRequestType::Change,
            )
            .await?;

        let change_request_with_entry = self
            .change_request_service
            .apply_change_entry(&change_request, &param.entry)
            .await?;

        self.change_request_service
            .commit_change_request(&change_request_with_entry)
            .await
    }

    pub async fn compact_files(&self, param: CompactFilesParam) -> Result<CommitId, anyhow::Error> {
        let change_request = self
            .change_request_service
            .find_or_create(
                &param.idempotency_key,
                &param.user_table_id,
                &param.stream_id,
                &param.partition_time,
                ChangeRequestType::Compact,
            )
            .await?;

        let change_request_with_entry = self
            .change_request_service
            .apply_compaction_entry(&change_request, &param.entry)
            .await?;

        self.change_request_service
            .commit_change_request(&change_request_with_entry)
            .await
    }
}
