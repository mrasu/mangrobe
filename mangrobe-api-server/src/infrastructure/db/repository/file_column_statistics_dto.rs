use crate::domain::model::file_id::FileId;
use crate::domain::model::file_column_statistics::FileColumnStatistics;
use crate::infrastructure::db::entity::file_column_statistics;
use sea_orm::Set;

pub fn build_entity_file_column_statistics(
    file_id: FileId,
    statistics: &FileColumnStatistics,
) -> file_column_statistics::ActiveModel {
    file_column_statistics::ActiveModel {
        id: Default::default(),
        file_id: Set(file_id.val()),
        column_name: Set(statistics.column_name.clone()),
        min: Set(statistics.min),
        max: Set(statistics.max),
        created_at: Default::default(),
        updated_at: Default::default(),
    }
}
