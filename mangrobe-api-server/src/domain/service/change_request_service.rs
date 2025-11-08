use crate::domain::model::change_request::{ChangeRequest, ChangeRequestStatus};
use crate::domain::model::change_request_change_file_entries::ChangeRequestChangeFileEntries;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::infrastructure::db::repository::change_request_file_add_entry_repository::ChangeRequestFileAddEntryRepository;
use crate::infrastructure::db::repository::change_request_repository::ChangeRequestRepository;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use crate::util::error::MangrobeError;
use anyhow::bail;
use sea_orm::sqlx::types::chrono::{DateTime, Utc};
use sea_orm::{DatabaseConnection, DatabaseTransaction, TransactionTrait};

pub struct ChangeRequestService {
    connection: DatabaseConnection,
    change_request_repository: ChangeRequestRepository,
    file_add_entry_repository: ChangeRequestFileAddEntryRepository,
    commit_repository: CommitRepository,
}

impl ChangeRequestService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            change_request_repository: ChangeRequestRepository::new(),
            file_add_entry_repository: ChangeRequestFileAddEntryRepository::new(),
            commit_repository: CommitRepository::new(),
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
        if !entries.add_entries.is_empty() {
            self.file_add_entry_repository
                .insert(txn, change_request, &entries.add_entries)
                .await?;
        }

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
