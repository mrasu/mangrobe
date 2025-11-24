use crate::domain::model::snapshot::Snapshot;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use crate::infrastructure::db::repository::current_file_repository::CurrentFileRepository;
use sea_orm::{AccessMode, DatabaseConnection, IsolationLevel, TransactionTrait};

pub struct SnapshotService {
    connection: DatabaseConnection,
    current_file_repository: CurrentFileRepository,
    commit_repository: CommitRepository,
}

impl SnapshotService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            current_file_repository: CurrentFileRepository::new(),
            commit_repository: CommitRepository::new(),
        }
    }

    pub async fn get_current(
        &self,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
    ) -> Result<Snapshot, anyhow::Error> {
        let txn = self
            .connection
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await?;

        let commit = self
            .commit_repository
            .find_latest(&txn, user_table_id, stream_id)
            .await?;

        let Some(commit) = commit else {
            return Ok(Snapshot::new(stream_id.clone(), None, vec![]));
        };

        let files = self
            .current_file_repository
            .find_files_by_stream(&txn, user_table_id, stream_id)
            .await?;

        Ok(Snapshot::new(stream_id.clone(), Some(commit.id), files))
    }
}
