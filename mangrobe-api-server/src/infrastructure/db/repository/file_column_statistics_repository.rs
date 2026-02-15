use crate::domain::model::file_column_statistics::FileColumnStatistics;
use crate::domain::model::file_id::FileId;
use crate::infrastructure::db::entity::file_column_statistics::Column;
use crate::infrastructure::db::entity::prelude::FileColumnStatistics as FileColumnStatisticsEntity;
use crate::infrastructure::db::repository::file_column_statistics_dto::{
    build_domain_column_statistics, build_entity_file_column_statistics,
};
use sea_orm::QueryFilter;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait};
use std::collections::HashMap;

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

    pub async fn find_file_column_stats_map_by_id<C: ConnectionTrait>(
        &self,
        conn: &C,
        file_ids: &[FileId],
    ) -> Result<HashMap<FileId, Vec<FileColumnStatistics>>, anyhow::Error> {
        let column_stats = FileColumnStatisticsEntity::find()
            .filter(Column::FileId.is_in(file_ids.iter().map(|f| f.val())))
            .all(conn)
            .await?;

        let mut stats_by_file_id: HashMap<FileId, Vec<FileColumnStatistics>> = HashMap::new();
        for stat in column_stats {
            stats_by_file_id
                .entry(FileId::from(stat.file_id))
                .or_default()
                .push(build_domain_column_statistics(stat));
        }

        Ok(stats_by_file_id)
    }
}
