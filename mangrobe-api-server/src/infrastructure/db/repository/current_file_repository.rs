use crate::domain::model::current_file::CurrentFile;
use crate::domain::model::file::{FilePath, FileWithId};
use crate::domain::model::file_id::FileId;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::partition_time_filter::{
    BoundInclusivity, PartitionTimeFilter, PartitionTimePredicate, PartitionTimeRange,
};
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::current_files::{Column, Entity};
use crate::infrastructure::db::entity::file_locks;
use crate::infrastructure::db::entity::prelude::{CurrentFiles, Files};
use crate::infrastructure::db::repository::current_file_dto::{
    build_domain_current_file, build_entity_current_file,
};
use crate::infrastructure::db::repository::file_dto::build_domain_file;
use crate::infrastructure::db::repository::file_repository::FileRepository;
use anyhow::bail;
use chrono::{DateTime, Utc};
use sea_orm::prelude::Expr;
use sea_orm::sea_query::{LockType, Query};
use sea_orm::{ColumnTrait, QuerySelect, Value};
use sea_orm::{Condition, QueryFilter};
use sea_orm::{ConnectionTrait, EntityTrait};
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
        stream: &UserTablStream,
        partition_time_filter: &PartitionTimeFilter,
    ) -> Result<Vec<FileWithId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let mut query = CurrentFiles::find()
            .find_also_related(Files)
            .filter(Column::UserTableId.eq(stream.user_table_id.val()))
            .filter(Column::StreamId.eq(stream.stream_id.val()));

        if partition_time_filter.should_filter() {
            let condition = build_partition_time_filter_condition(partition_time_filter);
            query = query.filter(condition);
        }

        let current_files = query.all(conn).await?;

        let result = current_files
            .iter()
            .filter_map(|(_current_file, file)| {
                let Some(file) = file else { return None };

                Some(build_domain_file(file))
            })
            .collect();

        Ok(result)
    }

    pub async fn select_locked_file_ids_for_update<C>(
        &self,
        conn: &C,
        file_lock_key: &FileLockKey,
        stream: &UserTablStream,
        file_ids: &[FileId],
    ) -> Result<Vec<FileId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let current_files = CurrentFiles::find()
            .lock(LockType::Update)
            .filter(Column::FileLockKey.eq(file_lock_key.key.clone()))
            .filter(Column::UserTableId.eq(stream.user_table_id.val()))
            .filter(Column::StreamId.eq(stream.stream_id.val()))
            .filter(Column::FileId.is_in(file_ids.iter().map(|v| v.val())))
            .all(conn)
            .await?;

        let file_ids = current_files.iter().map(|f| f.file_id.into()).collect();

        Ok(file_ids)
    }

    pub async fn select_files_by_paths_for_update<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        partition_time: DateTime<Utc>,
        file_paths: &[FilePath],
    ) -> Result<Vec<CurrentFile>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let hashed_file_paths: Vec<_> = file_paths.iter().map(|path| path.to_xxh3_128()).collect();

        let current_files = CurrentFiles::find()
            .lock(LockType::Update)
            .filter(Column::UserTableId.eq(stream.user_table_id.val()))
            .filter(Column::StreamId.eq(stream.stream_id.val()))
            .filter(Column::PartitionTime.eq(partition_time))
            .filter(Column::FilePathXxh3.is_in(hashed_file_paths))
            .all(conn)
            .await?;

        let files: Vec<_> = current_files
            .iter()
            .map(build_domain_current_file)
            .collect();

        Ok(files)
    }

    pub async fn acquire_lock<C>(
        &self,
        conn: &C,
        file_lock_key: &FileLockKey,
        stream: &UserTablStream,
        file_ids: &[FileId],
    ) -> Result<u64, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let locked_files = CurrentFiles::update_many()
            .col_expr(Column::FileLockKey, Expr::value(file_lock_key.key.clone()))
            .filter(Column::UserTableId.eq(stream.user_table_id.val()))
            .filter(Column::StreamId.eq(stream.stream_id.val()))
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

    pub async fn insert_many<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        file_ids: &[FileId],
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let saved_files = self
            .file_repository
            .find_files_by_ids(conn, stream, file_ids)
            .await?;
        let saved_files_map: HashMap<_, _> = saved_files.iter().map(|f| (f.id, f)).collect();

        let files: Result<Vec<_>, _> = file_ids
            .iter()
            .map(|file_id| {
                let Some(model) = saved_files_map.get(&file_id.val()) else {
                    bail!("file not found for save");
                };

                Ok(build_entity_current_file(
                    stream,
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

    pub async fn delete_many<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        file_ids: &[FileId],
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        Entity::delete_many()
            .filter(Column::UserTableId.eq(stream.user_table_id.val()))
            .filter(Column::StreamId.eq(stream.stream_id.val()))
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

fn build_partition_time_filter_condition(partition_time_filter: &PartitionTimeFilter) -> Condition {
    partition_time_filter
        .predicates
        .iter()
        .fold(Condition::any(), |condition, predicate| {
            condition.add(build_partition_time_predicate_condition(predicate))
        })
}

fn build_partition_time_predicate_condition(
    partition_time_predicate: &PartitionTimePredicate,
) -> Condition {
    match partition_time_predicate {
        PartitionTimePredicate::In(param) => {
            Condition::all().add(Column::PartitionTime.is_in(param.times.clone()))
        }
        PartitionTimePredicate::Range(param) => build_partition_time_range_condition(param),
    }
}

fn build_partition_time_range_condition(partition_time_range: &PartitionTimeRange) -> Condition {
    let mut condition = Condition::all();

    if let Some(lower) = &partition_time_range.lower {
        condition = condition.add(match lower.inclusivity {
            BoundInclusivity::Inclusive => Column::PartitionTime.gte(lower.time),
            BoundInclusivity::Exclusive => Column::PartitionTime.gt(lower.time),
        });
    }

    if let Some(upper) = &partition_time_range.upper {
        condition = condition.add(match upper.inclusivity {
            BoundInclusivity::Inclusive => Column::PartitionTime.lte(upper.time),
            BoundInclusivity::Exclusive => Column::PartitionTime.lt(upper.time),
        });
    }

    condition
}
