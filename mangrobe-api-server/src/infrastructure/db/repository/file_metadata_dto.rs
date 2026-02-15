use crate::domain::model::file_id::FileId;
use crate::domain::model::file_metadata::FileMetadata;
use crate::infrastructure::db::entity::file_metadata;
use crate::infrastructure::db::entity::file_metadata::Model;
use sea_orm::Set;

pub fn build_entity_file_metadata(
    file_id: FileId,
    metadata: &FileMetadata,
) -> file_metadata::ActiveModel {
    file_metadata::ActiveModel {
        file_id: Set(file_id.val()),
        parquet_metadata: Set(metadata.parquet_metadata.clone()),
        created_at: Default::default(),
        updated_at: Default::default(),
    }
}

pub(super) fn build_domain_file_metadata(model: Model) -> FileMetadata {
    FileMetadata::new(model.parquet_metadata)
}
