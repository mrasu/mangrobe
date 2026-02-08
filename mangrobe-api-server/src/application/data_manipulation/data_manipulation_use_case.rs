use crate::application::data_manipulation::add_files_param::AddFilesParam;
use crate::application::data_manipulation::change_files_param::ChangeFilesParam;
use crate::application::data_manipulation::compact_files_param::CompactFilesParam;
use crate::application::data_manipulation::get_changes_param::GetChangesParam;
use crate::application::data_manipulation::get_current_state_param::GetCurrentStateParam;
use crate::application::util::user_table::find_table_id;
use crate::domain::model::change_request::ChangeRequestType;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::committed_change_request::CommittedStreamChange;
use crate::domain::model::snapshot::Snapshot;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::domain::service::change_request_service::ChangeRequestService;
use crate::domain::service::committed_change_request_service::CommittedChangeRequestService;
use crate::domain::service::file_lock_key_service::FileLockService;
use crate::domain::service::snapshot_service::SnapshotService;
use crate::domain::service::user_table_service::UserTableService;
use crate::util::error::UserError;
use anyhow::bail;
use sea_orm::DatabaseConnection;

pub struct DataManipulationUseCase {
    snapshot_service: SnapshotService,
    change_request_service: ChangeRequestService,
    committed_change_request_service: CommittedChangeRequestService,
    file_lock_service: FileLockService,
    user_table_service: UserTableService,
}

impl DataManipulationUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            snapshot_service: SnapshotService::new(&connection),
            change_request_service: ChangeRequestService::new(&connection),
            committed_change_request_service: CommittedChangeRequestService::new(&connection),
            file_lock_service: FileLockService::new(&connection),
            user_table_service: UserTableService::new(&connection),
        }
    }

    pub async fn get_current_state(
        &self,
        param: GetCurrentStateParam,
    ) -> Result<Snapshot, anyhow::Error> {
        let table_id = find_table_id(&self.user_table_service, &param.table_name).await?;

        let stream = UserTablStream::new(table_id, param.stream_id);
        self.snapshot_service.get_current(&stream).await
    }

    pub async fn get_changes(
        &self,
        param: &GetChangesParam,
        limit_per_stream: u64,
    ) -> Result<CommittedStreamChange, anyhow::Error> {
        let table_id = find_table_id(&self.user_table_service, &param.table_name).await?;

        let changes = self
            .committed_change_request_service
            .get_after(
                &UserTablStream::new(table_id, param.stream_id.clone()),
                &param.commit_id_after,
                limit_per_stream,
            )
            .await;

        match changes {
            Ok(changes) => Ok(CommittedStreamChange::new(param.stream_id.clone(), changes)),
            Err(e) => Err(e),
        }
    }

    pub async fn add_files(&self, param: AddFilesParam) -> Result<CommitId, anyhow::Error> {
        let table_id = find_table_id(&self.user_table_service, &param.table_name).await?;
        let stream = UserTablStream::new(table_id, param.stream_id);
        let change_request = self
            .change_request_service
            .find_or_create(&param.idempotency_key, &stream, ChangeRequestType::AddFiles)
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

        let table_id = find_table_id(&self.user_table_service, &param.table_name).await?;
        let stream = UserTablStream::new(table_id, param.stream_id);
        let change_request = self
            .change_request_service
            .create(&stream, ChangeRequestType::Compact)
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

        let table_id = find_table_id(&self.user_table_service, &param.table_name).await?;
        let stream = UserTablStream::new(table_id, param.stream_id);
        let change_request = self
            .change_request_service
            .create(&stream, ChangeRequestType::Compact)
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
