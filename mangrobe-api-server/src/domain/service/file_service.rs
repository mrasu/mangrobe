use crate::domain::model::file_id::FileId;
use crate::domain::model::file_with_statistics::FileWithStatistics;
use crate::infrastructure::db::repository::file_column_statistics_repository::FileColumnStatisticsRepository;
use crate::infrastructure::db::repository::file_metadata_repository::FileMetadataRepository;
use crate::infrastructure::db::repository::file_repository::FileRepository;
use sea_orm::DatabaseConnection;

pub struct FileService {
    connection: DatabaseConnection,
    file_repository: FileRepository,
    file_column_statistics_repository: FileColumnStatisticsRepository,
    file_metadata_repository: FileMetadataRepository,
}

impl FileService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            file_repository: FileRepository::new(),
            file_column_statistics_repository: FileColumnStatisticsRepository::new(),
            file_metadata_repository: FileMetadataRepository::new(),
        }
    }

    pub async fn get_files_with_stat(
        &self,
        file_ids: &[FileId],
        includes_parquet_metadata: bool,
    ) -> Result<Vec<FileWithStatistics>, anyhow::Error> {
        let files = self
            .file_repository
            .find_all_files_by_ids(&self.connection, file_ids)
            .await?;

        let column_stats_by_file_id = self
            .file_column_statistics_repository
            .find_file_column_stats_map_by_id(&self.connection, file_ids)
            .await?;

        let metadata_by_file_id = self
            .file_metadata_repository
            .find_column_selected_metadata_map_by_id(
                &self.connection,
                file_ids,
                includes_parquet_metadata,
            )
            .await?;

        let res = files
            .into_iter()
            .map(|file| {
                let stats = column_stats_by_file_id
                    .get(&file.id)
                    .cloned()
                    .unwrap_or_default();
                let metadata = metadata_by_file_id.get(&file.id).cloned();
                FileWithStatistics::from_models(file, stats, metadata)
            })
            .collect();
        Ok(res)
    }
}
