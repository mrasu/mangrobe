use crate::domain::model::file_id::FileId;
use crate::domain::model::file_metadata::FileMetadata;
use crate::infrastructure::db::entity::file_metadata::Column;
use crate::infrastructure::db::entity::prelude::FileMetadata as FileMetadataEntity;
use crate::infrastructure::db::repository::file_metadata_dto::{
    build_domain_file_metadata, build_entity_file_metadata,
};
use sea_orm::QueryFilter;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QuerySelect};
use std::collections::HashMap;

pub struct FileMetadataRepository {}

impl FileMetadataRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn insert_many<C>(
        &self,
        conn: &C,
        metadata: &[(FileId, FileMetadata)],
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        if metadata.is_empty() {
            return Ok(());
        }

        let models = metadata
            .iter()
            .map(|(file_id, metadata)| build_entity_file_metadata(file_id.clone(), metadata));

        FileMetadataEntity::insert_many(models).exec(conn).await?;
        Ok(())
    }

    // Because metadata can be large, select only specified metadata column.
    pub async fn find_column_selected_metadata_map_by_id<C: ConnectionTrait>(
        &self,
        conn: &C,
        file_ids: &[FileId],
        includes_parquet_metadata: bool,
    ) -> Result<HashMap<FileId, FileMetadata>, anyhow::Error> {
        let mut query = FileMetadataEntity::find()
            .select_only()
            .column(Column::FileId)
            .column(Column::CreatedAt)
            .column(Column::UpdatedAt)
            .filter(Column::FileId.is_in(file_ids.iter().map(|f| f.val())));

        if includes_parquet_metadata {
            query = query.column(Column::ParquetMetadata);
        }

        let metadata_list = query.all(conn).await?;

        let res = metadata_list
            .into_iter()
            .map(|metadata| {
                (
                    FileId::from(metadata.file_id),
                    build_domain_file_metadata(metadata),
                )
            })
            .collect();

        Ok(res)
    }
}
