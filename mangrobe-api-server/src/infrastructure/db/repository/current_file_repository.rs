use crate::domain::model::file::File;
use crate::domain::model::file_id::FileId;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::infrastructure::db::entity::current_files::{ActiveModel, Column, Entity, Model};
use crate::infrastructure::db::entity::files;
use crate::infrastructure::db::entity::prelude::{CurrentFiles, Files};
use chrono::{DateTime, Utc};
use sea_orm::ColumnTrait;
use sea_orm::QueryFilter;
use sea_orm::{ConnectionTrait, EntityTrait, Set};

pub struct CurrentFileRepository {}

impl CurrentFileRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_files_by_stream<C>(
        &self,
        conn: &C,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
    ) -> Result<Vec<File>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let current_files = CurrentFiles::find()
            .find_also_related(Files)
            .filter(Column::UserTableId.eq(user_table_id.val()))
            .filter(Column::StreamId.eq(stream_id.val()))
            .all(conn)
            .await?;

        let result: Vec<File> = current_files
            .iter()
            .filter_map(|(current_file, file)| {
                let Some(file) = file else { return None };

                Some(self.new_file(current_file, file))
            })
            .collect();

        Ok(result)
    }

    fn new_file(&self, _current_file: &Model, file: &files::Model) -> File {
        File::new(
            file.user_table_id.into(),
            file.stream_id.into(),
            file.partition_time.into(),
            file.path.clone().into(),
            file.size,
        )
    }

    pub async fn insert_many<C>(
        &self,
        conn: &C,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        partition_time: DateTime<Utc>,
        file_ids: &[FileId],
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let files: Vec<_> = file_ids
            .iter()
            .map(|file_id| self.new_active_model(user_table_id, stream_id, partition_time, file_id))
            .collect();

        CurrentFiles::insert_many(files)
            .exec_with_returning_many(conn)
            .await?;

        Ok(())
    }

    fn new_active_model(
        &self,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        partition_time: DateTime<Utc>,
        file_id: &FileId,
    ) -> ActiveModel {
        ActiveModel {
            user_table_id: Set(user_table_id.val()),
            stream_id: Set(stream_id.val()),
            partition_time: Set(partition_time.into()),
            file_id: Set(file_id.val()),
            ..Default::default()
        }
    }

    pub async fn delete_many<C>(
        &self,
        conn: &C,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        partition_time: DateTime<Utc>,
        file_ids: &[FileId],
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        Entity::delete_many()
            .filter(Column::UserTableId.eq(user_table_id.val()))
            .filter(Column::StreamId.eq(stream_id.val()))
            .filter(Column::PartitionTime.eq(partition_time))
            .filter(Column::FileId.is_in(file_ids.iter().map(|v| v.val())))
            .exec(conn)
            .await?;

        Ok(())
    }
}
