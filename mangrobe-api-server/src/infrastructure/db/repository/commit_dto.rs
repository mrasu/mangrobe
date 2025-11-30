use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::commit::Commit;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::commits::{ActiveModel, Model};
use sea_orm::Set;

pub(super) fn build_entity_commit(
    stream: &UserTablStream,
    change_request_id: &ChangeRequestId,
) -> ActiveModel {
    ActiveModel {
        id: Default::default(),
        change_request_id: Set(change_request_id.into()),
        user_table_id: Set(stream.user_table_id.val()),
        stream_id: Set(stream.stream_id.val()),
        committed_at: Default::default(),
    }
}

pub(super) fn build_domain_commit(commit: &Model) -> Commit {
    Commit {
        id: commit.id.into(),
        change_request_id: commit.change_request_id.into(),
        stream: UserTablStream::new(commit.user_table_id.into(), commit.stream_id.into()),
        committed_at: commit.committed_at.into(),
    }
}
