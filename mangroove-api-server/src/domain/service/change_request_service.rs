use crate::domain::model::change_log::ChangeRequestChangeEntries;
use crate::domain::model::change_log_id::ChangeLogId;
use crate::domain::model::change_request::{ChangeRequest, ChangeRequestStatus};
use crate::infrastructure::db::repository::change_commit_repository::ChangeCommitRepository;
use crate::infrastructure::db::repository::change_request_file_add_entry_repository::ChangeRequestFileAddEntryRepository;
use crate::infrastructure::db::repository::change_request_repository::ChangeRequestRepository;
use crate::util::error::MangobeError;
use anyhow::bail;
use sea_orm::sqlx::types::chrono::{DateTime, Utc};
use sea_orm::{DatabaseConnection, DatabaseTransaction, TransactionTrait};

pub struct ChangeRequestService {
    connection: DatabaseConnection,
    change_request_repository: ChangeRequestRepository,
    file_add_entry_repository: ChangeRequestFileAddEntryRepository,
    commit_repository: ChangeCommitRepository,
}

impl ChangeRequestService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            change_request_repository: ChangeRequestRepository::new(),
            file_add_entry_repository: ChangeRequestFileAddEntryRepository::new(),
            commit_repository: ChangeCommitRepository::new(),
        }
    }

    pub async fn find_or_create(
        &self,
        idempotency_key: Vec<u8>,
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
        changed_entries: &ChangeRequestChangeEntries,
    ) -> Result<(), anyhow::Error> {
        let txn = self.connection.begin().await?;

        let status = self
            .change_request_repository
            .select_for_update(&txn, change_request)
            .await?;
        if status.is_completed(ChangeRequestStatus::ChangeInserted) {
            txn.commit().await?;
            return Ok(());
        } else if !status.can_progress_to(ChangeRequestStatus::ChangeInserted) {
            bail!(MangobeError::UnexpectedStateChange(
                status.to_string(),
                ChangeRequestStatus::ChangeInserted.to_string(),
            ));
        }

        self.insert_change_entries(&txn, change_request, changed_entries)
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
        changed_entries: &ChangeRequestChangeEntries,
    ) -> Result<(), anyhow::Error> {
        if !changed_entries.add_entries.is_empty() {
            self.file_add_entry_repository
                .insert(txn, change_request, &changed_entries.add_entries)
                .await?;
        }

        Ok(())
    }

    pub async fn commit_change_request(
        &self,
        change_request: &ChangeRequest,
    ) -> Result<ChangeLogId, anyhow::Error> {
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
            bail!(MangobeError::UnexpectedStateChange(
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
