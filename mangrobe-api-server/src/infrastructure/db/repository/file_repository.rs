use crate::domain::model::file::{File, FilePath, FileWithId};
use crate::domain::model::file_id::FileId;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::files;
use crate::infrastructure::db::entity::files::Column;
use crate::infrastructure::db::entity::prelude::Files;
use crate::infrastructure::db::repository::file_dto::{build_domain_file, build_entity_file};
use chrono::{DateTime, Utc};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

#[derive(Clone, Copy)]
pub struct FileRepository {}

impl FileRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_all_by_ids<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        ids: &[FileId],
    ) -> Result<Vec<FileWithId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let files = self.find_files_by_ids(conn, stream, ids).await?;

        let domain_files = files.iter().map(build_domain_file).collect();

        Ok(domain_files)
    }

    pub async fn find_all_ids_by_paths<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        partition_time: DateTime<Utc>,
        file_paths: &[FilePath],
    ) -> Result<Vec<FileId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let hashed_file_paths = file_paths.iter().map(|f| f.to_xxh3_128());

        let file_ids: Vec<FileId> = Files::find()
            .select_only()
            .column(Column::Id)
            .filter(Column::UserTableId.eq(stream.user_table_id.val()))
            .filter(Column::StreamId.eq(stream.stream_id.val()))
            .filter(Column::PartitionTime.eq(partition_time))
            .filter(Column::PathXxh3.is_in(hashed_file_paths))
            .into_tuple::<i64>()
            .all(conn)
            .await?
            .iter()
            .map(|i| (*i).into())
            .collect();

        Ok(file_ids)
    }

    pub(super) async fn find_files_by_ids<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        ids: &[FileId],
    ) -> Result<Vec<files::Model>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let files = Files::find()
            .filter(Column::UserTableId.eq(stream.user_table_id.val()))
            .filter(Column::StreamId.eq(stream.stream_id.val()))
            .filter(Column::Id.is_in(ids.iter().map(|f| f.val())))
            .all(conn)
            .await?;

        Ok(files)
    }

    pub async fn insert<C>(&self, conn: &C, target_file: &File) -> Result<FileId, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let file = build_entity_file(target_file);
        let inserted = Files::insert(file).exec_with_returning(conn).await?;

        Ok(inserted.id.into())
    }

    pub async fn insert_many<C>(
        &self,
        conn: &C,
        target_files: &[File],
    ) -> Result<Vec<FileId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let files = target_files.iter().map(build_entity_file);

        let inserted = Files::insert_many(files)
            .exec_with_returning_many(conn)
            .await?;

        let file_ids: Vec<FileId> = inserted.iter().map(|i| i.id.into()).collect();
        Ok(file_ids)
    }
}
