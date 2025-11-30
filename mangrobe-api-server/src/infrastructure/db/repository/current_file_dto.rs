use crate::domain::model::current_file::CurrentFile;
use crate::domain::model::file::FilePath;
use crate::domain::model::file_id::FileId;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::current_files::{ActiveModel, Model};
use chrono::{DateTime, Utc};
use sea_orm::Set;

pub(super) fn build_entity_current_file(
    stream: &UserTablStream,
    partition_time: DateTime<Utc>,
    file_id: &FileId,
    file_path: &FilePath,
) -> ActiveModel {
    ActiveModel {
        id: Default::default(),
        user_table_id: Set(stream.user_table_id.val()),
        stream_id: Set(stream.stream_id.val()),
        partition_time: Set(partition_time.into()),
        file_id: Set(file_id.val()),
        file_path_xxh3: Set(file_path.to_xxh3_128()),
        file_lock_key: Set(None),
        created_at: Default::default(),
        updated_at: Default::default(),
    }
}

pub(super) fn build_domain_current_file(current_file: &Model) -> CurrentFile {
    CurrentFile::new(current_file.file_id.into())
}
