use crate::domain::model::current_file::CurrentFile;
use crate::domain::model::file::{File, FilePath};
use crate::domain::model::file_id::FileId;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::infrastructure::db::entity::current_files::{ActiveModel, Column, Entity, Model};
use crate::infrastructure::db::entity::prelude::{CurrentFiles, Files};
use crate::infrastructure::db::entity::{file_locks, files};
use crate::infrastructure::db::repository::file_repository::FileRepository;
use anyhow::bail;
use chrono::{DateTime, Utc};
use sea_orm::prelude::Expr;
use sea_orm::sea_query::{LockType, Query};
use sea_orm::{ColumnTrait, QuerySelect, Value};
use sea_orm::{Condition, QueryFilter};
use sea_orm::{ConnectionTrait, EntityTrait, Set};
use std::collections::HashMap;

pub struct CurrentFileRepository {
    file_repository: FileRepository,
}

impl CurrentFileRepository {
    pub fn new() -> Self {
        Self {
            file_repository: FileRepository::new(),
        }
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

    pub async fn select_locked_file_ids_for_update<C>(
        &self,
        conn: &C,
        file_lock_key: &FileLockKey,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        file_ids: &[FileId],
    ) -> Result<Vec<FileId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let current_files = CurrentFiles::find()
            .lock(LockType::Update)
            .filter(Column::FileLockKey.eq(file_lock_key.key.clone()))
            .filter(Column::UserTableId.eq(user_table_id.val()))
            .filter(Column::StreamId.eq(stream_id.val()))
            .filter(Column::FileId.is_in(file_ids.iter().map(|v| v.val())))
            .all(conn)
            .await?;

        let file_ids = current_files.iter().map(|f| f.file_id.into()).collect();

        Ok(file_ids)
    }

    pub async fn select_files_by_paths_for_update<C>(
        &self,
        conn: &C,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        partition_time: DateTime<Utc>,
        file_paths: &[FilePath],
    ) -> Result<Vec<CurrentFile>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let hashed_file_paths: Vec<_> = file_paths.iter().map(|path| path.to_xxh3_128()).collect();

        let current_files = CurrentFiles::find()
            .lock(LockType::Update)
            .filter(Column::UserTableId.eq(user_table_id.val()))
            .filter(Column::StreamId.eq(stream_id.val()))
            .filter(Column::PartitionTime.eq(partition_time))
            .filter(Column::FilePathXxh3.is_in(hashed_file_paths))
            .all(conn)
            .await?;

        let files: Vec<_> = current_files
            .iter()
            .map(|current_file| self.new_current_file(current_file))
            .collect();

        Ok(files)
    }

    pub async fn acquire_lock<C>(
        &self,
        conn: &C,
        file_lock_key: &FileLockKey,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        file_ids: &[FileId],
    ) -> Result<u64, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let locked_files = CurrentFiles::update_many()
            .col_expr(Column::FileLockKey, Expr::value(file_lock_key.key.clone()))
            .filter(Column::UserTableId.eq(user_table_id.val()))
            .filter(Column::StreamId.eq(stream_id.val()))
            .filter(Column::FileId.is_in(file_ids.iter().map(|id| id.val())))
            .filter(
                // ... AND ((key IS NULL) OR (key NOT IN (select key from locks where expired_at > now()))
                Condition::any().add(Column::FileLockKey.is_null()).add(
                    Column::FileLockKey.not_in_subquery(
                        Query::select()
                            .column(file_locks::Column::Key)
                            .from(file_locks::Entity)
                            .and_where(file_locks::Column::ExpireAt.gte(Utc::now()))
                            .to_owned(),
                    ),
                ),
            )
            .exec(conn)
            .await?;

        Ok(locked_files.rows_affected)
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

    fn new_current_file(&self, current_file: &Model) -> CurrentFile {
        CurrentFile::new(current_file.file_id.into())
    }

    pub async fn insert_many<C>(
        &self,
        conn: &C,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        file_ids: &[FileId],
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let saved_files = self
            .file_repository
            .find_files_by_ids(conn, user_table_id, stream_id, file_ids)
            .await?;
        let saved_files_map: HashMap<_, _> = saved_files.iter().map(|f| (f.id, f)).collect();

        let files: Result<Vec<_>, _> = file_ids
            .iter()
            .map(|file_id| {
                let Some(model) = saved_files_map.get(&file_id.val()) else {
                    bail!("file not found for save");
                };

                Ok(self.new_active_model(
                    user_table_id,
                    stream_id,
                    model.partition_time.into(),
                    file_id,
                    &model.path.clone().into(),
                ))
            })
            .collect();

        CurrentFiles::insert_many(files?)
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
        file_path: &FilePath,
    ) -> ActiveModel {
        ActiveModel {
            id: Default::default(),
            user_table_id: Set(user_table_id.val()),
            stream_id: Set(stream_id.val()),
            partition_time: Set(partition_time.into()),
            file_id: Set(file_id.val()),
            file_path_xxh3: Set(file_path.to_xxh3_128()),
            file_lock_key: Set(None),
            created_at: Default::default(),
            updated_at: Default::default(),
        }
    }

    pub async fn delete_many<C>(
        &self,
        conn: &C,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        file_ids: &[FileId],
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        Entity::delete_many()
            .filter(Column::UserTableId.eq(user_table_id.val()))
            .filter(Column::StreamId.eq(stream_id.val()))
            .filter(Column::FileId.is_in(file_ids.iter().map(|v| v.val())))
            .exec(conn)
            .await?;

        Ok(())
    }

    pub async fn release_lock<C>(
        &self,
        conn: &C,
        file_lock_key: &FileLockKey,
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        Entity::update_many()
            .col_expr(Column::FileLockKey, Expr::value(Value::Bytes(None)))
            .filter(Column::FileLockKey.eq(file_lock_key.key.clone()))
            .exec(conn)
            .await?;

        Ok(())
    }
}
