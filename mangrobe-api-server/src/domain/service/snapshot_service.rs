use crate::domain::model::change_file_entry::ChangeFileEntry;
use crate::domain::model::snapshot::Snapshot;
use crate::infrastructure::db::repository::change_request_file_entry_repository::ChangeRequestFileEntryRepository;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use crate::infrastructure::db::repository::file_repository::FileRepository;
use sea_orm::DatabaseConnection;

pub struct SnapshotService {
    connection: DatabaseConnection,
    commit_repository: CommitRepository,
    change_request_file_add_entry_repository: ChangeRequestFileEntryRepository,
    file_repository: FileRepository,
}

impl SnapshotService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            commit_repository: CommitRepository::new(),
            change_request_file_add_entry_repository: ChangeRequestFileEntryRepository::new(),
            file_repository: FileRepository::new(),
        }
    }

    pub async fn get_latest(&self) -> Result<Snapshot, anyhow::Error> {
        // TODO: get "checkout" and changes after the checkout
        let change_request_ids = self
            .commit_repository
            .fetch_change_request_ids_for_latest(&self.connection)
            .await?;

        let change_file_entries = self
            .change_request_file_add_entry_repository
            .find_all_by_change_request_ids(&self.connection, &change_request_ids)
            .await?;

        let active_file_ids =
            ChangeFileEntry::pick_active_file_ids(&change_request_ids, change_file_entries);
        let files = self
            .file_repository
            .find_all_by_id(&self.connection, active_file_ids)
            .await?;

        let snapshot = Snapshot { files };
        Ok(snapshot)
    }
}
