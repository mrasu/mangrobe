use crate::domain::model::file::FileWithStatistics;
use crate::domain::model::file_id::FileId;
use crate::infrastructure::db::repository::file_repository::FileRepository;
use sea_orm::DatabaseConnection;

pub struct FileService {
    connection: DatabaseConnection,
    file_repository: FileRepository,
}

impl FileService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            file_repository: FileRepository::new(),
        }
    }

    pub async fn get_files_with_stat(
        &self,
        file_ids: &[FileId],
    ) -> Result<Vec<FileWithStatistics>, anyhow::Error> {
        self.file_repository
            .find_files_with_statics_by_ids(&self.connection, file_ids)
            .await
    }
}
