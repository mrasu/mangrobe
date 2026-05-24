use crate::domain::model::stream::Stream;
use crate::domain::model::stream_info::StreamInfo;
use crate::domain::model::user_table_id::UserTableId;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use sea_orm::DatabaseConnection;

pub(crate) struct StreamService {
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
        stream: &Option<Stream>,
        limit: u64,
    ) -> Result<Vec<StreamInfo>, anyhow::Error> {
        self.commit_repository
            .find_streams_after(&self.connection, table_id, stream, limit)
            .await
    }
}
