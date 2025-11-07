use crate::domain::model::change_request_change_file_entries::ChangeRequestChangeFileEntries;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::snapshot::Snapshot;
use crate::domain::service::change_request_service::ChangeRequestService;
use crate::domain::service::snapshot_service::SnapshotService;
use chrono::{DateTime, Utc};
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

    pub async fn change_files(
        &self,
        idempotency_key: Vec<u8>,
        tenant_id: i64,
        partition_time: DateTime<Utc>,
        entries: &ChangeRequestChangeFileEntries,
    ) -> Result<CommitId, anyhow::Error> {
        let change_request = self
            .change_request_service
            .find_or_create(idempotency_key, tenant_id, partition_time)
            .await?;

        self.change_request_service
            .insert_entries(&change_request, &entries)
            .await?;

        self.change_request_service
            .commit_change_request(&change_request)
            .await
    }
}
