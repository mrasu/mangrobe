use crate::domain::model::file::File;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::files;
use crate::infrastructure::db::entity::files::ActiveModel;
use sea_orm::Set;

pub(super) fn build_entity_file(file: &File) -> ActiveModel {
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

pub(super) fn build_domain_file(file: &files::Model) -> File {
    File::new(
        UserTablStream::new(file.user_table_id.into(), file.stream_id.into()),
        file.partition_time.into(),
        file.path.clone().into(),
        file.size,
    )
}
