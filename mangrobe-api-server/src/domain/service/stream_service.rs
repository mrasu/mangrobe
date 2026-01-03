use crate::domain::model::stream::Stream;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use sea_orm::DatabaseConnection;

pub struct StreamService {
    connection: DatabaseConnection,
    commit_repository: CommitRepository,
}

impl StreamService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            commit_repository: CommitRepository::new(),
        }
    }

    pub async fn find_streams_after(
        &self,
        table_id: &UserTableId,
        stream_id: &Option<StreamId>,
        limit: u64,
    ) -> Result<Vec<Stream>, anyhow::Error> {
        self.commit_repository
            .find_streams_after(&self.connection, table_id, stream_id, limit)
            .await
    }
}
