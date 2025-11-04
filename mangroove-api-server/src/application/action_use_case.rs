use crate::domain::change_log::ChangedFiles;
use crate::domain::change_log_id::ChangeLogId;
use crate::infrastructure::db::entity::prelude::{
    ChangeRequestAddFiles, ChangeRequestIdempotencyKeys, ChangeRequests,
};
use crate::infrastructure::db::entity::{change_commits, change_request_add_files};
use crate::infrastructure::db::entity::{change_request_idempotency_keys, change_requests};
use sea_orm::sqlx::types::chrono::{DateTime, Utc};
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, Set};
use sea_orm::{QueryFilter, TryInsertResult};
use std::time::Duration;

pub struct ActionUseCase {
    connection: DatabaseConnection,
}

impl ActionUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self { connection }
    }

    pub async fn change_files(
        &self,
        idempotency_key: Vec<u8>,
        tenant_id: i64,
        partition_time: DateTime<Utc>,
        changed_files: &ChangedFiles,
    ) -> Result<ChangeLogId, Box<dyn std::error::Error>> {
        let change_request = self
            .find_or_create_change_request(idempotency_key, tenant_id, partition_time)
            .await?;

        let changed_files =
            changed_files
                .added_files
                .iter()
                .map(|file| change_request_add_files::ActiveModel {
                    change_request_id: Set(change_request.id),
                    path: Set(file.path.clone()),
                    size: Set(file.size),
                    ..Default::default()
                });
        ChangeRequestAddFiles::insert_many(changed_files)
            .exec(&self.connection)
            .await?;

        // TODO: more wise when adding compaction or expiration

        // TODO: lock to not race with other change.
        let commit = change_commits::ActiveModel {
            change_request_id: Set(change_request.id),
            ..Default::default()
        }
        .insert(&self.connection)
        .await?;

        Ok(ChangeLogId::from(commit.id))
    }

    pub async fn find_or_create_change_request(
        &self,
        idempotency_key: Vec<u8>,
        tenant_id: i64,
        partition_time: DateTime<Utc>,
    ) -> Result<change_requests::Model, Box<dyn std::error::Error>> {
        let existing_key = ChangeRequestIdempotencyKeys::find()
            .filter(change_request_idempotency_keys::Column::Key.eq(idempotency_key.clone()))
            .one(&self.connection)
            .await?;

        if let Some(existing_key) = existing_key {
            let change_request = ChangeRequests::find()
                .filter(change_requests::Column::Id.eq(existing_key.change_request_id))
                .one(&self.connection)
                .await?;
            if let Some(change_request) = change_request {
                return Ok(change_request);
            }
            return Err(
                "invalid state found. idempotency key doesn't belong any change requests".into(),
            );
        }

        let change_request = change_requests::ActiveModel {
            tenant_id: Set(tenant_id),
            partition_time: Set(partition_time.into()),
            ..Default::default()
        };
        let change_request = change_request.insert(&self.connection).await?;

        let new_idempotency_key = change_request_idempotency_keys::ActiveModel {
            change_request_id: Set(change_request.id),
            key: Set(idempotency_key.clone()),
            expires_at: Set((Utc::now() + Duration::from_secs(24 * 3600 * 7)).into()),
            ..Default::default()
        };
        let result = change_request_idempotency_keys::Entity::insert(new_idempotency_key)
            .on_conflict_do_nothing()
            .exec_without_returning(&self.connection)
            .await?;
        if let TryInsertResult::Inserted(_) = result {
            return Ok(change_request);
        }

        let existing_key = ChangeRequestIdempotencyKeys::find()
            .filter(change_request_idempotency_keys::Column::Key.eq(idempotency_key.clone()))
            .one(&self.connection)
            .await?;
        let Some(existing_key) = existing_key else {
            return Err(
                "invalid situation. cannot add idempotency key but no change request for it".into(),
            );
        };

        let change_request = ChangeRequests::find()
            .filter(change_requests::Column::Id.eq(existing_key.change_request_id))
            .one(&self.connection)
            .await?;
        if let Some(change_request) = change_request {
            return Ok(change_request);
        }

        Err("invalid state found. idempotency key doesn't belong any change requests".into())
    }
}
