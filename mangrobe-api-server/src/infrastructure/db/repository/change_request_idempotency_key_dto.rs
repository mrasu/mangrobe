use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::infrastructure::db::entity::change_request_idempotency_keys::ActiveModel;
use chrono::{DateTime, Utc};
use sea_orm::Set;

pub(super) fn build_entity_change_request_idempotency_key(
    key: &IdempotencyKey,
    change_request_id: &ChangeRequestId,
    expires_at: DateTime<Utc>,
) -> ActiveModel {
    ActiveModel {
        change_request_id: Set(change_request_id.val()),
        key: Set(key.vec()),
        expires_at: Set(expires_at.into()),
        created_at: Default::default(),
        updated_at: Default::default(),
    }
}
