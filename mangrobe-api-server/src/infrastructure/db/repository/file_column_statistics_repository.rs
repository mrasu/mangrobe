use crate::domain::model::file_column_statistics::FileColumnStatistics;
use crate::domain::model::file_id::FileId;
use crate::infrastructure::db::entity::prelude::FileColumnStatistics as FileColumnStatisticsEntity;
use crate::infrastructure::db::repository::file_column_statistics_dto::build_entity_file_column_statistics;
use sea_orm::{ConnectionTrait, EntityTrait};

pub struct FileColumnStatisticsRepository {}

impl FileColumnStatisticsRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn insert_many<C>(
        &self,
        conn: &C,
        statistics: &[(FileId, FileColumnStatistics)],
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        if statistics.is_empty() {
            return Ok(());
        }

        let models = statistics.iter().map(|(file_id, statistics)| {
            build_entity_file_column_statistics(file_id.clone(), statistics)
        });

        FileColumnStatisticsEntity::insert_many(models)
            .exec(conn)
            .await?;
        Ok(())
    }
}
