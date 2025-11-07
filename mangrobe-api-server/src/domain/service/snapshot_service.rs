use crate::domain::model::file::File;
use crate::domain::model::snapshot::Snapshot;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use crate::infrastructure::db::repository::change_request_file_add_entry_repository::ChangeRequestFileAddEntryRepository;
use sea_orm::DatabaseConnection;

pub struct SnapshotService {
    connection: DatabaseConnection,
    commit_repository: CommitRepository,
    change_request_file_add_entry_repository: ChangeRequestFileAddEntryRepository,
}

impl SnapshotService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            commit_repository: CommitRepository::new(),
            change_request_file_add_entry_repository: ChangeRequestFileAddEntryRepository::new(),
        }
    }

    pub async fn get_latest(&self) -> Result<Snapshot, anyhow::Error> {
        // TODO: get "checkout" and changes after the checkout
        let change_request_ids = self
            .commit_repository
            .fetch_change_request_ids_for_latest(&self.connection)
            .await?;

        let files = self
            .change_request_file_add_entry_repository
            .find_by_change_request_ids(&self.connection, change_request_ids)
            .await?;

        let snapshot = Snapshot {
            files: files
                .iter()
                .map(|f| File {
                    path: f.path.clone(),
                    size: f.size,
                })
                .collect(),
        };
        Ok(snapshot)
    }
}
