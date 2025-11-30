use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::commit::Commit;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::commits::{ActiveModel, Column, Entity, Model};
use crate::infrastructure::db::entity::prelude::Commits;
use crate::util::error::MangrobeError;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QueryOrder, Set,
};

pub struct CommitRepository {}

impl CommitRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_latest<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
    ) -> Result<Option<Commit>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let commit = Entity::find()
            .filter(Column::UserTableId.eq(stream.user_table_id.val()))
            .filter(Column::StreamId.eq(stream.stream_id.val()))
            .order_by_desc(Column::Id)
            .one(conn)
            .await?;

        let Some(commit) = commit else {
            return Ok(None);
        };

        Ok(Some(self.build_domain_commit(&commit)))
    }

    fn build_domain_commit(&self, commit: &Model) -> Commit {
        Commit {
            id: commit.id.into(),
            change_request_id: commit.change_request_id.into(),
            stream: UserTablStream::new(commit.user_table_id.into(), commit.stream_id.into()),
            committed_at: commit.committed_at.into(),
        }
    }

    pub async fn insert<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        change_request_id: &ChangeRequestId,
    ) -> Result<CommitId, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let commit = ActiveModel {
            id: Default::default(),
            change_request_id: Set(change_request_id.into()),
            user_table_id: Set(stream.user_table_id.val()),
            stream_id: Set(stream.stream_id.val()),
            committed_at: Default::default(),
        }
        .insert(conn)
        .await?;

        Ok(commit.id.into())
    }

    pub async fn find_by_change_request_id(
        &self,
        txn: &DatabaseTransaction,
        change_request_id: ChangeRequestId,
    ) -> Result<CommitId, anyhow::Error> {
        let commit = Commits::find()
            .filter(Column::ChangeRequestId.eq(change_request_id.val()))
            .one(txn)
            .await?
            .ok_or(MangrobeError::UnexpectedState(
                "no commit found but marked as commited.".to_string(),
            ))?;

        Ok(commit.id.into())
    }
}
