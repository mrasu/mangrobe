use crate::domain::model::change_request::{
    ChangeRequest, ChangeRequestAbs, ChangeRequestStatus, ChangeRequestType,
    ChangeRequestWithFileEntry,
};
use crate::domain::model::change_request_file_entry::ChangeRequestFileEntry::AddFiles;
use crate::domain::model::change_request_raw_file_entry::{
    ChangeRequestRawAddFilesEntry, ChangeRequestRawChangeFilesEntry,
    ChangeRequestRawCompactFilesEntry,
};
use crate::domain::model::changeset::Changeset;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::file::{FileEntry, FilePath};
use crate::domain::model::file_id::FileId;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::infrastructure::db::repository::change_request_repository::ChangeRequestRepository;
use crate::infrastructure::db::repository::commit_lock_repository::CommitLockRepository;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use crate::infrastructure::db::repository::current_file_repository::CurrentFileRepository;
use crate::infrastructure::db::repository::file_repository::FileRepository;
use crate::util::error::MangrobeError;
use crate::util::error::MangrobeError::UnexpectedState;
use anyhow::bail;
use sea_orm::sqlx::types::chrono::{DateTime, Utc};
use sea_orm::{DatabaseConnection, DatabaseTransaction, TransactionTrait};

pub struct ChangeRequestService {
    connection: DatabaseConnection,
    change_request_repository: ChangeRequestRepository,
    commit_repository: CommitRepository,
    commit_lock_repository: CommitLockRepository,
    file_repository: FileRepository,
    current_file_repository: CurrentFileRepository,
}

impl ChangeRequestService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            change_request_repository: ChangeRequestRepository::new(),
            commit_repository: CommitRepository::new(),
            commit_lock_repository: CommitLockRepository::new(),
            file_repository: FileRepository::new(),
            current_file_repository: CurrentFileRepository::new(),
        }
    }

    pub async fn find_or_create(
        &self,
        idempotency_key: &IdempotencyKey,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        partition_time: &DateTime<Utc>,
        change_type: ChangeRequestType,
    ) -> Result<ChangeRequest, anyhow::Error> {
        self.change_request_repository
            .find_or_create(
                &self.connection,
                idempotency_key,
                user_table_id,
                stream_id,
                partition_time,
                change_type,
            )
            .await
    }

    pub async fn apply_add_entry(
        &self,
        change_request: &ChangeRequest,
        entry: &ChangeRequestRawAddFilesEntry,
    ) -> Result<ChangeRequestWithFileEntry, anyhow::Error> {
        let txn = self.connection.begin().await?;

        // TODO: make ChangeRequest state machine
        let change_request = self
            .change_request_repository
            .select_for_update(&txn, change_request)
            .await?;
        let status = &change_request.status;
        if status.is_completed(ChangeRequestStatus::ChangeInserted) {
            let Some(ref entry) = change_request.file_entry else {
                bail!(UnexpectedState(
                    "no entry is saved for the change_request".into()
                ));
            };
            match entry {
                AddFiles { add_files } => return Ok(change_request.with_file_entry(entry.clone())),
                _ => bail!(UnexpectedState(
                    "not AddFile entry is saved for the changed_request".into()
                )),
            }
        } else if !status.can_progress_to(ChangeRequestStatus::ChangeInserted) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::ChangeInserted.to_string(),
            ));
        }

        let file_ids = self
            .insert_files(&txn, &change_request, &entry.files)
            .await?;

        let modified_request = self
            .update_file_entry_as_add(&txn, &change_request, &file_ids)
            .await?;

        let modified_request = self
            .change_request_repository
            .update_status(&txn, &modified_request, ChangeRequestStatus::ChangeInserted)
            .await?;

        txn.commit().await?;

        Ok(modified_request)
    }

    async fn update_file_entry_as_add(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        file_ids_to_add: &[FileId],
    ) -> Result<ChangeRequestWithFileEntry, anyhow::Error> {
        let entry = self
            .change_request_repository
            .update_add_file_entry(txn, change_request, file_ids_to_add)
            .await?;

        Ok(change_request.with_file_entry(entry))
    }

    pub async fn apply_change_entry(
        &self,
        change_request: &ChangeRequest,
        entry: &ChangeRequestRawChangeFilesEntry,
    ) -> Result<ChangeRequestWithFileEntry, anyhow::Error> {
        let txn = self.connection.begin().await?;

        // TODO: make ChangeRequest state machine
        let change_request = self
            .change_request_repository
            .select_for_update(&txn, change_request)
            .await?;
        let status = change_request.status;
        if status.is_completed(ChangeRequestStatus::ChangeInserted) {
            return Ok(change_request.unwrap_to_with_file_entry());
        } else if !status.can_progress_to(ChangeRequestStatus::ChangeInserted) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::ChangeInserted.to_string(),
            ));
        }

        let file_ids_to_add = self
            .insert_files(&txn, &change_request, &entry.files_to_add)
            .await?;

        let modified_request = self
            .update_file_entry_as_change(&txn, &change_request, &file_ids_to_add)
            .await?;

        let modified_request = self
            .change_request_repository
            .update_status(&txn, &modified_request, ChangeRequestStatus::ChangeInserted)
            .await?;

        txn.commit().await?;

        Ok(modified_request)
    }

    async fn insert_files(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        files_to_add: &[FileEntry],
    ) -> Result<Vec<FileId>, anyhow::Error> {
        if files_to_add.is_empty() {
            return Ok(vec![]);
        };

        let files: Vec<_> = files_to_add
            .iter()
            .map(|f| {
                f.to_file(
                    &change_request.user_table_id,
                    &change_request.stream_id,
                    change_request.partition_time,
                )
            })
            .collect();
        let file_ids = self.file_repository.insert_many(txn, &files).await?;

        Ok(file_ids)
    }

    async fn update_file_entry_as_change(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        file_ids_to_add: &[FileId],
    ) -> Result<ChangeRequestWithFileEntry, anyhow::Error> {
        let entry = self
            .change_request_repository
            .update_change_file_entry(txn, change_request, file_ids_to_add)
            .await?;

        Ok(change_request.with_file_entry(entry))
    }

    pub async fn apply_compaction_entry(
        &self,
        change_request: &ChangeRequest,
        entry: &ChangeRequestRawCompactFilesEntry,
    ) -> Result<ChangeRequestWithFileEntry, anyhow::Error> {
        let txn = self.connection.begin().await?;

        // TODO: make ChangeRequest state machine
        let change_request = self
            .change_request_repository
            .select_for_update(&txn, change_request)
            .await?;
        let status = change_request.status;
        if status.is_completed(ChangeRequestStatus::ChangeInserted) {
            return Ok(change_request.unwrap_to_with_file_entry());
        } else if !status.can_progress_to(ChangeRequestStatus::ChangeInserted) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::ChangeInserted.to_string(),
            ));
        }

        let src_file_ids = self
            .find_file_ids(&txn, &change_request, &entry.src_file_paths)
            .await?;
        let dst_file_id = self
            .insert_file(&txn, &change_request, &entry.dst_file)
            .await?;

        let modified_request = self
            .update_file_entry_as_compaction(&txn, &change_request, src_file_ids, dst_file_id)
            .await?;

        let modified_request = self
            .change_request_repository
            .update_status(&txn, &modified_request, ChangeRequestStatus::ChangeInserted)
            .await?;

        txn.commit().await?;

        Ok(modified_request)
    }

    async fn insert_file(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        file_entry: &FileEntry,
    ) -> Result<FileId, anyhow::Error> {
        let file = file_entry.to_file(
            &change_request.user_table_id,
            &change_request.stream_id,
            change_request.partition_time,
        );

        self.file_repository.insert(txn, &file).await
    }

    async fn find_file_ids(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        file_paths: &[FilePath],
    ) -> Result<Vec<FileId>, anyhow::Error> {
        self.file_repository
            .find_all_ids_by_paths(
                txn,
                &change_request.stream_id,
                change_request.partition_time,
                file_paths,
            )
            .await
    }

    async fn update_file_entry_as_compaction(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        src_file_ids: Vec<FileId>,
        dst_file_id: FileId,
    ) -> Result<ChangeRequestWithFileEntry, anyhow::Error> {
        let entry = self
            .change_request_repository
            .update_compact_file_entry(txn, change_request, &src_file_ids, &dst_file_id)
            .await?;

        Ok(change_request.with_file_entry(entry))
    }

    pub async fn commit_change_request(
        &self,
        change_request: &ChangeRequestWithFileEntry,
    ) -> Result<CommitId, anyhow::Error> {
        let txn = self.connection.begin().await?;

        let current_change_request = self
            .change_request_repository
            .select_for_update(&txn, change_request)
            .await?;
        let status = current_change_request.status;

        // TODO: make ChangeRequest state machine
        if status.is_completed(ChangeRequestStatus::Committed) {
            return self
                .commit_repository
                .find_by_change_request_id(&txn, current_change_request.id.clone())
                .await;
        } else if !status.can_progress_to(ChangeRequestStatus::Committed) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::Committed.to_string(),
            ));
        }

        let changeset = change_request.to_changeset();
        let commit_id = self
            .commit_changeset(&txn, change_request, &changeset)
            .await?;

        self.change_request_repository
            .update_status(&txn, change_request, ChangeRequestStatus::Committed)
            .await?;

        txn.commit().await?;

        Ok(commit_id)
    }

    async fn commit_changeset(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequestWithFileEntry,
        changeset: &Changeset,
    ) -> Result<CommitId, anyhow::Error> {
        self.commit_lock_repository
            .acquire_xact_lock(
                txn,
                &change_request.user_table_id,
                &change_request.stream_id,
            )
            .await?;

        if !changeset.add_file_ids.is_empty() {
            self.current_file_repository
                .insert_many(
                    txn,
                    &change_request.user_table_id,
                    &change_request.stream_id,
                    change_request.partition_time,
                    &changeset.add_file_ids,
                )
                .await?;
        }

        if !changeset.delete_file_ids.is_empty() {
            self.current_file_repository
                .delete_many(
                    txn,
                    &change_request.user_table_id,
                    &change_request.stream_id,
                    change_request.partition_time,
                    &changeset.delete_file_ids,
                )
                .await?;
        }

        let commit_id = self
            .commit_repository
            .insert(
                txn,
                &change_request.user_table_id,
                &change_request.stream_id,
                &change_request.id,
            )
            .await?;

        Ok(commit_id)
    }
}
