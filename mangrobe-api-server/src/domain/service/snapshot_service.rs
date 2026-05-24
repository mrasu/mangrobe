use crate::domain::model::partition_filter::PartitionFilter;
use crate::domain::model::snapshot::Snapshot;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use crate::infrastructure::db::repository::current_file_repository::CurrentFileRepository;
use sea_orm::{AccessMode, DatabaseConnection, IsolationLevel, TransactionTrait};

pub(crate) struct SnapshotService {
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
        stream: &UserTablStream,
        partition_filter: &PartitionFilter,
    ) -> Result<Snapshot, anyhow::Error> {
        let txn = self
            .connection
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await?;

        let commit = self.commit_repository.find_latest(&txn, stream).await?;

        let Some(commit) = commit else {
            return Ok(Snapshot::new(
                stream.clone(),
                None,
                vec![],
                partition_filter.data_type.clone(),
            ));
        };

        let files = self
            .current_file_repository
            .find_files_by_stream(&txn, stream, partition_filter)
            .await?;

        Ok(Snapshot::new(
            stream.clone(),
            Some(commit.id),
            files,
            partition_filter.data_type.clone(),
        ))
    }
}
