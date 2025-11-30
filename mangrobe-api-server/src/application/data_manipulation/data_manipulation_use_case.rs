use crate::application::data_manipulation::add_files_param::AddFilesParam;
use crate::application::data_manipulation::change_files_param::ChangeFilesParam;
use crate::application::data_manipulation::compact_files_param::CompactFilesParam;
use crate::application::data_manipulation::get_current_snapshot_param::GetCurrentSnapshotParam;
use crate::domain::model::change_request::ChangeRequestType;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::snapshot::Snapshot;
use crate::domain::service::change_request_service::ChangeRequestService;
use crate::domain::service::file_lock_key_service::FileLockService;
use crate::domain::service::snapshot_service::SnapshotService;
use crate::util::error::UserError;
use anyhow::bail;
use sea_orm::DatabaseConnection;

pub struct DataManipulationUseCase {
    snapshot_service: SnapshotService,
    change_request_service: ChangeRequestService,
    file_lock_service: FileLockService,
}

impl DataManipulationUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            snapshot_service: SnapshotService::new(&connection),
            change_request_service: ChangeRequestService::new(&connection),
            file_lock_service: FileLockService::new(&connection),
        }
    }

    pub async fn get_current_snapshot(
        &self,
        param: GetCurrentSnapshotParam,
    ) -> Result<Snapshot, anyhow::Error> {
        self.snapshot_service.get_current(&param.stream).await
    }

    pub async fn add_files(&self, param: AddFilesParam) -> Result<CommitId, anyhow::Error> {
        let change_request = self
            .change_request_service
            .find_or_create(
                &param.idempotency_key,
                &param.stream,
                ChangeRequestType::AddFiles,
            )
            .await?;

        let mut change_request_with_entry = self
            .change_request_service
            .apply_add_entries(&change_request, &param.entries)
            .await?;

        self.change_request_service
            .commit_add_only_change_request(&mut change_request_with_entry)
            .await
    }

    pub async fn change_files(&self, param: ChangeFilesParam) -> Result<CommitId, anyhow::Error> {
        let lock_exists = self
            .file_lock_service
            .check_existence(&param.file_lock_key)
            .await?;
        if !lock_exists {
            bail!(UserError::InvalidLockMessage("not acquired".into()))
        }

        let change_request = self
            .change_request_service
            .create(&param.stream, ChangeRequestType::Compact)
            .await?;

        let mut change_request_with_entry = self
            .change_request_service
            .apply_change_entry(&change_request, &param.entries)
            .await?;

        let changeset = change_request_with_entry.to_changeset();
        self.change_request_service
            .commit_change_request(
                &param.file_lock_key,
                &mut change_request_with_entry.base,
                &changeset,
            )
            .await
    }

    pub async fn compact_files(&self, param: CompactFilesParam) -> Result<CommitId, anyhow::Error> {
        let lock_exists = self
            .file_lock_service
            .check_existence(&param.file_lock_key)
            .await?;
        if !lock_exists {
            bail!(UserError::InvalidLockMessage("not acquired".into()))
        }

        let change_request = self
            .change_request_service
            .create(&param.stream, ChangeRequestType::Compact)
            .await?;

        let mut change_request_with_entry = self
            .change_request_service
            .apply_compaction_entry(&change_request, &param.entries)
            .await?;

        let changeset = change_request_with_entry.to_changeset();
        self.change_request_service
            .commit_change_request(
                &param.file_lock_key,
                &mut change_request_with_entry.base,
                &changeset,
            )
            .await
    }
}
