use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::file_locks::{Column, Entity};
use crate::infrastructure::db::entity::prelude::FileLocks;
use crate::infrastructure::db::repository::file_lock_dto::build_entity_file_lock;
use chrono::{Duration, Utc};
use sea_orm::{ActiveModelTrait, ColumnTrait, TryInsertResult};
use sea_orm::{ConnectionTrait, EntityTrait, QueryFilter};

pub struct FileLockRepository {}

impl FileLockRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn exists<C>(&self, conn: &C, key: &FileLockKey) -> Result<bool, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let lock = FileLocks::find()
            .filter(Column::Key.eq(key.key.clone()))
            .filter(Column::ExpireAt.gte(key.request_started_at))
            .one(conn)
            .await?;

        Ok(lock.is_some())
    }

    pub async fn acquire<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        ttl: Duration,
        key: &FileLockKey,
    ) -> Result<bool, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let lock = build_entity_file_lock(key, stream, Utc::now() + ttl);

        let result = Entity::insert(lock)
            .on_conflict_do_nothing()
            .exec(conn)
            .await?;

        let inserted = match result {
            TryInsertResult::Empty => false,
            TryInsertResult::Conflicted => false,
            TryInsertResult::Inserted(_) => true,
        };

        Ok(inserted)
    }

    pub async fn release<C>(&self, conn: &C, key: &FileLockKey) -> Result<bool, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let result = FileLocks::delete_by_id(key.key.clone()).exec(conn).await?;

        Ok(result.rows_affected > 0)
    }
}
