use crate::domain::model::file::{File, FilePath};
use crate::domain::model::file_id::FileId;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::files;
use crate::infrastructure::db::entity::files::{ActiveModel, Column};
use crate::infrastructure::db::entity::prelude::Files;
use chrono::{DateTime, Utc};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Set};

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
    ) -> Result<Vec<File>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let files = self.find_files_by_ids(conn, stream, ids).await?;

        let domain_files = files.iter().map(|f| self.build_domain_file(f)).collect();

        Ok(domain_files)
    }

    fn build_domain_file(&self, file: &files::Model) -> File {
        File::new(
            UserTablStream::new(file.user_table_id.into(), file.stream_id.into()),
            file.partition_time.into(),
            file.path.clone().into(),
            file.size,
        )
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
        let file = self.new_active_model(target_file);
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
        let files = target_files.iter().map(|f| self.new_active_model(f));

        let inserted = Files::insert_many(files)
            .exec_with_returning_many(conn)
            .await?;

        let file_ids: Vec<FileId> = inserted.iter().map(|i| i.id.into()).collect();
        Ok(file_ids)
    }

    fn new_active_model(&self, file: &File) -> ActiveModel {
        ActiveModel {
            id: Default::default(),
            user_table_id: Set(file.stream.user_table_id.val()),
            stream_id: Set(file.stream.stream_id.val()),
            partition_time: Set(file.partition_time.into()),
            path: Set(file.path.path()),
            path_xxh3: Set(file.path.to_xxh3_128()),
            size: Set(file.size),
            created_at: Default::default(),
            updated_at: Default::default(),
        }
    }
}
