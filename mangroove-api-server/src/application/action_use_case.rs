use crate::domain::model::change_log::ChangeRequestChangeEntries;
use crate::domain::model::change_log_id::ChangeLogId;
use crate::domain::service::change_request_service::ChangeRequestService;
use sea_orm::DatabaseConnection;
use sea_orm::sqlx::types::chrono::{DateTime, Utc};

pub struct ActionUseCase {
    change_request_service: ChangeRequestService,
}

impl ActionUseCase {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            change_request_service: ChangeRequestService::new(connection),
        }
    }

    pub async fn change_files(
        &self,
        idempotency_key: Vec<u8>,
        tenant_id: i64,
        partition_time: DateTime<Utc>,
        changed_files: &ChangeRequestChangeEntries,
    ) -> Result<ChangeLogId, anyhow::Error> {
        let change_request = self
            .change_request_service
            .find_or_create(idempotency_key, tenant_id, partition_time)
            .await?;

        self.change_request_service
            .insert_entries(&change_request, &changed_files)
            .await?;

        self.change_request_service
            .commit_change_request(&change_request)
            .await
    }
}
