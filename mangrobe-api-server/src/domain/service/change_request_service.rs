use crate::domain::model::change_file_entry::{
    ChangeFileAddEntry, ChangeFileCompactEntry, ChangeFileEntry,
};
use crate::domain::model::change_request::{ChangeRequest, ChangeRequestStatus, ChangeRequestType};
use crate::domain::model::change_request_change_file_entries::ChangeRequestChangeFileEntries;
use crate::domain::model::change_request_compact_file_entry::ChangeRequestCompactFileEntry;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::infrastructure::db::repository::change_request_file_entry_repository::ChangeRequestFileEntryRepository;
use crate::infrastructure::db::repository::change_request_repository::ChangeRequestRepository;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use crate::infrastructure::db::repository::file_repository::FileRepository;
use crate::util::error::MangrobeError;
use anyhow::bail;
use sea_orm::sqlx::types::chrono::{DateTime, Utc};
use sea_orm::{DatabaseConnection, DatabaseTransaction, TransactionTrait};

pub struct ChangeRequestService {
    connection: DatabaseConnection,
    change_request_repository: ChangeRequestRepository,
    change_request_file_entry_repository: ChangeRequestFileEntryRepository,
    commit_repository: CommitRepository,
    file_repository: FileRepository,
}

impl ChangeRequestService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            change_request_repository: ChangeRequestRepository::new(),
            change_request_file_entry_repository: ChangeRequestFileEntryRepository::new(),
            commit_repository: CommitRepository::new(),
            file_repository: FileRepository::new(),
        }
    }

    pub async fn find_or_create(
        &self,
        idempotency_key: IdempotencyKey,
        tenant_id: i64,
        partition_time: DateTime<Utc>,
    ) -> Result<ChangeRequest, anyhow::Error> {
        self.change_request_repository
            .find_or_create(&self.connection, idempotency_key, tenant_id, partition_time)
            .await
    }

    pub async fn insert_entries(
        &self,
        change_request: &ChangeRequest,
        entries: &ChangeRequestChangeFileEntries,
    ) -> Result<(), anyhow::Error> {
        let txn = self.connection.begin().await?;

        // TODO: make ChangeRequest state machine
        let status = self
            .change_request_repository
            .select_for_update(&txn, change_request)
            .await?;
        if status.is_completed(ChangeRequestStatus::ChangeInserted) {
            return Ok(());
        } else if !status.can_progress_to(ChangeRequestStatus::ChangeInserted) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::ChangeInserted.to_string(),
            ));
        }

        self.insert_change_entries(&txn, change_request, entries)
            .await?;

        self.change_request_repository
            .update_status(&txn, change_request, ChangeRequestStatus::ChangeInserted)
            .await?;

        txn.commit().await?;

        Ok(())
    }

    async fn insert_change_entries(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        entries: &ChangeRequestChangeFileEntries,
    ) -> Result<(), anyhow::Error> {
        let mut change_file_entries = Vec::<ChangeFileEntry>::new();

        if !entries.add_entries.is_empty() {
            let file_ids = self
                .file_repository
                .insert_many(txn, &entries.add_entries)
                .await?;

            change_file_entries.extend(file_ids.iter().map(|file_id| ChangeFileEntry::Add {
                add: ChangeFileAddEntry {
                    file_id: file_id.clone(),
                },
            }))
        }

        self.change_request_file_entry_repository
            .insert(
                txn,
                change_request,
                ChangeRequestType::Change,
                &change_file_entries,
            )
            .await?;

        Ok(())
    }

    pub async fn insert_compaction_entry(
        &self,
        change_request: &ChangeRequest,
        entry: &ChangeRequestCompactFileEntry,
    ) -> Result<(), anyhow::Error> {
        let txn = self.connection.begin().await?;

        // TODO: make ChangeRequest state machine
        let status = self
            .change_request_repository
            .select_for_update(&txn, change_request)
            .await?;
        if status.is_completed(ChangeRequestStatus::ChangeInserted) {
            return Ok(());
        } else if !status.can_progress_to(ChangeRequestStatus::ChangeInserted) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::ChangeInserted.to_string(),
            ));
        }

        self.apply_compaction_entry(&txn, change_request, entry)
            .await?;

        self.change_request_repository
            .update_status(&txn, change_request, ChangeRequestStatus::ChangeInserted)
            .await?;

        txn.commit().await?;

        Ok(())
    }

    async fn apply_compaction_entry(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        entry: &ChangeRequestCompactFileEntry,
    ) -> Result<(), anyhow::Error> {
        let dst_file_id = self.file_repository.insert(txn, &entry.dst_file()).await?;
        let src_file_ids = self
            .file_repository
            .find_all_ids_by_locator(
                txn,
                entry.tenant_id,
                entry.partition_time,
                &entry.src_entries,
            )
            .await?;

        let change_file_entry = ChangeFileEntry::Compact {
            compact: ChangeFileCompactEntry {
                src_file_ids,
                dst_file_id,
            },
        };

        self.change_request_file_entry_repository
            .insert(
                txn,
                change_request,
                ChangeRequestType::Compact,
                &vec![change_file_entry],
            )
            .await?;

        Ok(())
    }

    pub async fn commit_change_request(
        &self,
        change_request: &ChangeRequest,
    ) -> Result<CommitId, anyhow::Error> {
        // TODO: lock to not race with other change.

        let txn = self.connection.begin().await?;
        let status = self
            .change_request_repository
            .select_for_update(&txn, change_request)
            .await?;

        if status.is_completed(ChangeRequestStatus::Committed) {
            return self
                .commit_repository
                .get_by_change_request_id(&txn, change_request.id.clone())
                .await;
        } else if !status.can_progress_to(ChangeRequestStatus::Committed) {
            bail!(MangrobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::Committed.to_string(),
            ));
        }

        let change_log_id = self
            .commit_repository
            .insert(&txn, change_request.id.clone())
            .await?;

        self.change_request_repository
            .update_status(&txn, change_request, ChangeRequestStatus::Committed)
            .await?;

        txn.commit().await?;

        Ok(change_log_id)
    }
}
