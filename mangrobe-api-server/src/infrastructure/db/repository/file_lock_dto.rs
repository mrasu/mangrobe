use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::file_locks::ActiveModel;
use chrono::{DateTime, Utc};
use sea_orm::Set;

pub(super) fn build_entity_file_lock(
    key: &FileLockKey,
    stream: &UserTablStream,
    expire_at: DateTime<Utc>,
) -> ActiveModel {
    ActiveModel {
        key: Set(key.key.clone()),
        user_table_id: Set(stream.user_table_id.val()),
        stream_id: Set(stream.stream_id.val()),
        expire_at: Set(expire_at.into()),
        created_at: Default::default(),
        updated_at: Default::default(),
    }
}
