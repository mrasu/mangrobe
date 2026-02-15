use crate::domain::model::change_request::{
    BaseChangeRequest, ChangeRequest, ChangeRequestForAdd, ChangeRequestForChange,
    ChangeRequestForCompact, ChangeRequestStatus, ChangeRequestTrait, ChangeRequestType,
};
use crate::domain::model::change_request_file_entry::ChangeRequestFileEntry::{
    AddFiles, ChangeFiles, Compact,
};
use crate::domain::model::change_request_file_entry::{
    ChangeRequestCompactFileEntry, ChangeRequestFileEntry,
};
use crate::domain::model::change_request_raw_file_entry::{
    ChangeRequestRawAddFileEntry, ChangeRequestRawChangeFilesEntry,
    ChangeRequestRawCompactFilesEntry,
};
use crate::domain::model::changeset::Changeset;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::file::{FileEntry, FilePath};
use crate::domain::model::file_id::FileId;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::repository::change_request_repository::ChangeRequestRepository;
use crate::infrastructure::db::repository::commit_lock_repository::CommitLockRepository;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use crate::infrastructure::db::repository::current_file_repository::CurrentFileRepository;
use crate::infrastructure::db::repository::file_column_statistics_repository::FileColumnStatisticsRepository;
use crate::infrastructure::db::repository::file_lock_repository::FileLockRepository;
use crate::infrastructure::db::repository::file_metadata_repository::FileMetadataRepository;
use crate::infrastructure::db::repository::file_repository::FileRepository;
use crate::util::error::MangrobeError::UnexpectedState;
use crate::util::error::{MangrobeError, UserError};
use anyhow::bail;
use sea_orm::sqlx::types::chrono::{DateTime, Utc};
use sea_orm::{DatabaseConnection, DatabaseTransaction, TransactionTrait};

pub struct ChangeRequestService {
    connection: DatabaseConnection,
    file_lock_repository: FileLockRepository,
    change_request_repository: ChangeRequestRepository,
    commit_repository: CommitRepository,
    commit_lock_repository: CommitLockRepository,
    file_repository: FileRepository,
    file_column_statistics_repository: FileColumnStatisticsRepository,
    file_metadata_repository: FileMetadataRepository,
    current_file_repository: CurrentFileRepository,
}

impl ChangeRequestService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            file_lock_repository: FileLockRepository::new(),
            change_request_repository: ChangeRequestRepository::new(),
            commit_repository: CommitRepository::new(),
            commit_lock_repository: CommitLockRepository::new(),
            file_repository: FileRepository::new(),
            file_column_statistics_repository: FileColumnStatisticsRepository::new(),
            file_metadata_repository: FileMetadataRepository::new(),
            current_file_repository: CurrentFileRepository::new(),
        }
    }

    pub async fn find_or_create(
        &self,
        idempotency_key: &IdempotencyKey,
        stream: &UserTablStream,
        change_type: ChangeRequestType,
    ) -> Result<ChangeRequest, anyhow::Error> {
        self.change_request_repository
            .find_by_idempotency_key_or_create(
                &self.connection,
                idempotency_key,
                stream,
                change_type,
            )
            .await
    }

    pub async fn create(
        &self,
        stream: &UserTablStream,
        change_type: ChangeRequestType,
    ) -> Result<ChangeRequest, anyhow::Error> {
        self.change_request_repository
            .create(&self.connection, stream, change_type)
            .await
    }

    pub async fn apply_add_entries(
        &self,
        change_request: &ChangeRequest,
        entries: &[ChangeRequestRawAddFileEntry],
    ) -> Result<ChangeRequestForAdd, anyhow::Error> {
        let txn = self.connection.begin().await?;

        if let Some(entry) = self
            .select_entry_of_change_request_for_update(
                &txn,
                change_request,
                ChangeRequestStatus::ChangeInserted,
            )
            .await?
        {
            match entry {
                AddFiles { add_files } => {
                    return Ok(ChangeRequestForAdd::new(
                        change_request.base.clone(),
                        add_files.clone(),
                    ));
                }
                _ => bail!(UnexpectedState(
                    "not AddFile entry is saved for the changed_request".into()
                )),
            }
        }

        let mut file_ids = vec![];
        for entry in entries {
            file_ids.extend(
                self.insert_files(
                    &txn,
                    change_request,
                    entry.partition_time,
                    &entry.files_to_add,
                )
                .await?,
            );
        }

        let mut add_request = self
            .update_file_entry_as_add(&txn, change_request.clone(), &file_ids)
            .await?;

        self.change_request_repository
            .update_status(&txn, &mut add_request, ChangeRequestStatus::ChangeInserted)
            .await?;

        txn.commit().await?;

        Ok(add_request)
    }

    async fn update_file_entry_as_add(
        &self,
        txn: &DatabaseTransaction,
        change_request: ChangeRequest,
        file_ids_to_add: &[FileId],
    ) -> Result<ChangeRequestForAdd, anyhow::Error> {
        let base = change_request.base.clone();
        let entry = self
            .change_request_repository
            .update_add_file_entry(txn, change_request, file_ids_to_add)
            .await?;

        Ok(ChangeRequestForAdd::new(base, entry))
    }

    pub async fn apply_change_entry(
        &self,
        change_request: &ChangeRequest,
        entries: &[ChangeRequestRawChangeFilesEntry],
    ) -> Result<ChangeRequestForChange, anyhow::Error> {
        let txn = self.connection.begin().await?;

        if let Some(entry) = self
            .select_entry_of_change_request_for_update(
                &txn,
                change_request,
                ChangeRequestStatus::ChangeInserted,
            )
            .await?
        {
            match entry {
                ChangeFiles { change_files } => {
                    return Ok(ChangeRequestForChange::new(
                        change_request.base.clone(),
                        change_files.clone(),
                    ));
                }
                _ => bail!(UnexpectedState(
                    "not ChangeFile entry is saved for the changed_request".into()
                )),
            }
        }

        let mut file_ids_to_delete = vec![];
        for entry in entries {
            file_ids_to_delete.extend(
                self.find_file_ids(
                    &txn,
                    change_request,
                    entry.partition_time,
                    &entry.files_to_delete,
                )
                .await?,
            );
        }

        let mut change_request = self
            .update_file_entry_as_change(&txn, change_request.clone(), &file_ids_to_delete)
            .await?;

        self.change_request_repository
            .update_status(
                &txn,
                &mut change_request,
                ChangeRequestStatus::ChangeInserted,
            )
            .await?;

        txn.commit().await?;

        Ok(change_request)
    }

    async fn insert_files(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        partition_time: DateTime<Utc>,
        files_to_add: &[FileEntry],
    ) -> Result<Vec<FileId>, anyhow::Error> {
        if files_to_add.is_empty() {
            return Ok(vec![]);
        };

        let files: Vec<_> = files_to_add
            .iter()
            .map(|f| f.to_file(change_request.base.stream.clone(), partition_time))
            .collect();
        let file_ids = self.file_repository.insert_many(txn, &files).await?;

        let statistics_to_insert: Vec<_> = file_ids
            .iter()
            .zip(files_to_add.iter())
            .flat_map(|(file_id, entry)| {
                entry
                    .column_statistics
                    .iter()
                    .cloned()
                    .map(|statistics| (file_id.clone(), statistics))
                    .collect::<Vec<_>>()
            })
            .collect();
        self.file_column_statistics_repository
            .insert_many(txn, &statistics_to_insert)
            .await?;

        let metadata_to_insert: Vec<_> = file_ids
            .iter()
            .zip(files_to_add.iter())
            .filter_map(|(file_id, entry)| {
                entry
                    .file_metadata
                    .clone()
                    .map(|metadata| (file_id.clone(), metadata))
            })
            .collect();
        self.file_metadata_repository
            .insert_many(txn, &metadata_to_insert)
            .await?;

        Ok(file_ids)
    }

    async fn update_file_entry_as_change(
        &self,
        txn: &DatabaseTransaction,
        change_request: ChangeRequest,
        file_ids_to_delete: &[FileId],
    ) -> Result<ChangeRequestForChange, anyhow::Error> {
        let base = change_request.base.clone();
        let entry = self
            .change_request_repository
            .update_change_file_entry(txn, change_request, file_ids_to_delete)
            .await?;

        Ok(ChangeRequestForChange::new(base, entry))
    }

    pub async fn apply_compaction_entry(
        &self,
        change_request: &ChangeRequest,
        entries: &[ChangeRequestRawCompactFilesEntry],
    ) -> Result<ChangeRequestForCompact, anyhow::Error> {
        let txn = self.connection.begin().await?;

        if let Some(entry) = self
            .select_entry_of_change_request_for_update(
                &txn,
                change_request,
                ChangeRequestStatus::ChangeInserted,
            )
            .await?
        {
            match entry {
                Compact { compact } => {
                    return Ok(ChangeRequestForCompact::new(
                        change_request.base.clone(),
                        compact.clone(),
                    ));
                }
                _ => bail!(UnexpectedState(
                    "not ChangeFile entry is saved for the changed_request".into()
                )),
            }
        }

        let mut compact_entries = vec![];
        for entry in entries {
            for info_entry in entry.info_entries.iter() {
                // TODO: accelerate by batch
                let src_file_ids = self
                    .find_file_ids(
                        &txn,
                        change_request,
                        entry.partition_time,
                        &info_entry.src_file_paths,
                    )
                    .await?;

                let dst_file_id = self
                    .insert_file(
                        &txn,
                        change_request,
                        entry.partition_time,
                        &info_entry.dst_file,
                    )
                    .await?;
                compact_entries.push(ChangeRequestCompactFileEntry {
                    src_file_ids,
                    dst_file_id,
                })
            }
        }

        let mut compaction_request = self
            .update_file_entry_as_compaction(&txn, change_request.clone(), &compact_entries)
            .await?;

        self.change_request_repository
            .update_status(
                &txn,
                &mut compaction_request,
                ChangeRequestStatus::ChangeInserted,
            )
            .await?;

        txn.commit().await?;

        Ok(compaction_request)
    }

    async fn select_entry_of_change_request_for_update<CR>(
        &self,
        txn: &DatabaseTransaction,
        change_request: &CR,
        next_status: ChangeRequestStatus,
    ) -> Result<Option<ChangeRequestFileEntry>, anyhow::Error>
    where
        CR: ChangeRequestTrait,
    {
        let change_request = self
            .change_request_repository
            .select_for_update(txn, change_request)
            .await?;
        let status = change_request.base.status;
        if status.is_completed(next_status) {
            let Some(entry) = change_request.file_entry else {
                bail!(UnexpectedState(
                    "no entry is saved for the change_request".into()
                ));
            };
            return Ok(Some(entry));
        } else if !status.can_progress_to(next_status) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                next_status.to_string(),
            ));
        }

        Ok(None)
    }

    async fn insert_file(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        partition_time: DateTime<Utc>,
        file_entry: &FileEntry,
    ) -> Result<FileId, anyhow::Error> {
        let file = file_entry.to_file(change_request.base.stream.clone(), partition_time);

        let file_id = self.file_repository.insert(txn, &file).await?;

        if !file_entry.column_statistics.is_empty() {
            let stats_to_insert: Vec<_> = file_entry
                .column_statistics
                .iter()
                .cloned()
                .map(|statistics| (file_id.clone(), statistics))
                .collect();
            self.file_column_statistics_repository
                .insert_many(txn, &stats_to_insert)
                .await?;
        }

        if let Some(metadata) = file_entry.file_metadata.clone() {
            self.file_metadata_repository
                .insert_many(txn, &[(file_id.clone(), metadata)])
                .await?;
        }

        Ok(file_id)
    }

    async fn find_file_ids(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        partition_time: DateTime<Utc>,
        file_paths: &[FilePath],
    ) -> Result<Vec<FileId>, anyhow::Error> {
        self.file_repository
            .find_all_ids_by_paths(txn, &change_request.base.stream, partition_time, file_paths)
            .await
    }

    async fn update_file_entry_as_compaction(
        &self,
        txn: &DatabaseTransaction,
        change_request: ChangeRequest,
        compact_entries: &[ChangeRequestCompactFileEntry],
    ) -> Result<ChangeRequestForCompact, anyhow::Error> {
        let base = change_request.base.clone();
        let entry = self
            .change_request_repository
            .update_compact_file_entry(txn, change_request, compact_entries)
            .await?;

        Ok(ChangeRequestForCompact::new(base, entry))
    }

    pub async fn commit_add_only_change_request(
        &self,
        change_request: &mut ChangeRequestForAdd,
    ) -> Result<CommitId, anyhow::Error> {
        let txn = self.connection.begin().await?;

        let current_change_request = self
            .change_request_repository
            .select_for_update(&txn, change_request)
            .await?;
        let status = current_change_request.base.status;

        // TODO: make ChangeRequest state machine
        if status.is_completed(ChangeRequestStatus::Committed) {
            return self
                .commit_repository
                .find_by_change_request_id(&txn, current_change_request.base.id.clone())
                .await;
        } else if !status.can_progress_to(ChangeRequestStatus::Committed) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::Committed.to_string(),
            ));
        }

        let commit_id = self.commit_add_only_change(&txn, change_request).await?;

        self.change_request_repository
            .update_status(&txn, change_request, ChangeRequestStatus::Committed)
            .await?;

        txn.commit().await?;

        Ok(commit_id)
    }

    async fn commit_add_only_change(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequestForAdd,
    ) -> Result<CommitId, anyhow::Error> {
        self.commit_lock_repository
            .acquire_xact_lock(txn, &change_request.base.stream)
            .await?;

        self.current_file_repository
            .insert_many(
                txn,
                &change_request.base.stream,
                &change_request.change_files_entry.file_ids,
            )
            .await?;

        let commit_id = self
            .commit_repository
            .insert(txn, &change_request.base.stream, &change_request.base.id)
            .await?;

        Ok(commit_id)
    }

    pub async fn commit_change_request(
        &self,
        file_lock_key: &FileLockKey,
        base_change_request: &mut BaseChangeRequest,
        changeset: &Changeset,
    ) -> Result<CommitId, anyhow::Error> {
        let txn = self.connection.begin().await?;

        let current_change_request = self
            .change_request_repository
            .select_for_update(&txn, base_change_request)
            .await?;
        let status = current_change_request.base.status;

        // TODO: make ChangeRequest state machine
        if status.is_completed(ChangeRequestStatus::Committed) {
            return self
                .commit_repository
                .find_by_change_request_id(&txn, current_change_request.base.id.clone())
                .await;
        } else if !status.can_progress_to(ChangeRequestStatus::Committed) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::Committed.to_string(),
            ));
        }

        let commit_id = self
            .commit_changeset(&txn, file_lock_key, base_change_request, changeset)
            .await?;

        self.change_request_repository
            .update_status(&txn, base_change_request, ChangeRequestStatus::Committed)
            .await?;

        txn.commit().await?;

        Ok(commit_id)
    }

    async fn commit_changeset(
        &self,
        txn: &DatabaseTransaction,
        file_lock_key: &FileLockKey,
        base_change_request: &BaseChangeRequest,
        changeset: &Changeset,
    ) -> Result<CommitId, anyhow::Error> {
        self.commit_lock_repository
            .acquire_xact_lock(txn, &base_change_request.stream)
            .await?;

        let lock_exists = self
            .file_lock_repository
            .exists(&self.connection, file_lock_key)
            .await?;
        if !lock_exists {
            bail!(UserError::InvalidLockMessage("not found".into()));
        }

        if !changeset.add_file_ids.is_empty() {
            self.current_file_repository
                .insert_many(txn, &base_change_request.stream, &changeset.add_file_ids)
                .await?;
        }

        if !changeset.delete_file_ids.is_empty() {
            let locked_file_ids = self
                .current_file_repository
                .select_locked_file_ids_for_update(
                    txn,
                    file_lock_key,
                    &base_change_request.stream,
                    &changeset.delete_file_ids,
                )
                .await?;

            if !FileId::has_same(&changeset.delete_file_ids, &locked_file_ids) {
                bail!(UserError::InvalidLockMessage(
                    "not locked file found".into()
                ));
            }

            self.current_file_repository
                .delete_many(txn, &base_change_request.stream, &changeset.delete_file_ids)
                .await?;
        }

        let commit_id = self
            .commit_repository
            .insert(txn, &base_change_request.stream, &base_change_request.id)
            .await?;

        self.current_file_repository
            .release_lock(txn, file_lock_key)
            .await?;

        let deleted = self
            .file_lock_repository
            .release(txn, file_lock_key)
            .await?;
        if !deleted {
            bail!("failed to delete lock after checking existence");
        }

        Ok(commit_id)
    }
}
