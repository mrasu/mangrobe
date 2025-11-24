use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::infrastructure::db::entity::change_request_idempotency_keys::{
    ActiveModel, Column, Entity, Model,
};
use crate::infrastructure::db::entity::prelude::ChangeRequestIdempotencyKeys;
use chrono::{DateTime, Utc};
use sea_orm::QueryFilter;
use sea_orm::{ColumnTrait, ConnectionTrait};
use sea_orm::{EntityTrait, Set, TryInsertResult};

// ChangeRequestIdempotencyKeyRepository is only for other infra repositories. Must not be used from domain.
pub(super) struct ChangeRequestIdempotencyKeyRepository {}

impl ChangeRequestIdempotencyKeyRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_by_key<C>(
        &self,
        conn: &C,
        idempotency_key: &IdempotencyKey,
    ) -> Result<Option<Model>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let key = ChangeRequestIdempotencyKeys::find()
            .filter(Column::Key.eq(idempotency_key.vec()))
            .one(conn)
            .await?;

        Ok(key)
    }

    pub async fn insert_if_no_exists<C>(
        &self,
        conn: &C,
        key: &IdempotencyKey,
        change_request_id: &ChangeRequestId,
        expires_at: DateTime<Utc>,
    ) -> Result<Option<Model>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let new_idempotency_key = ActiveModel {
            change_request_id: Set(change_request_id.val()),
            key: Set(key.vec()),
            expires_at: Set(expires_at.into()),
            ..Default::default()
        };
        let result = Entity::insert(new_idempotency_key)
            .on_conflict_do_nothing()
            .exec_with_returning(conn)
            .await?;

        let TryInsertResult::Inserted(inserted) = result else {
            return Ok(None);
        };

        Ok(Some(inserted))
    }
}
