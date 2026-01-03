use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::commit::Commit;
use crate::domain::model::commit_id::CommitId;
use crate::domain::model::committed_change_request::CommittedChangeRequest;
use crate::domain::model::stream::Stream;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::commits::{Column, Entity};
use crate::infrastructure::db::entity::prelude::{ChangeRequests, Commits};
use crate::infrastructure::db::entity_ext::change_request_ext::ChangeRequestExt;
use crate::infrastructure::db::repository::commit_dto::{build_domain_commit, build_entity_commit};
use crate::util::error::MangrobeError;
use sea_orm::prelude::Expr;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect,
};

#[derive(Clone, Copy)]
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

        Ok(Some(build_domain_commit(&commit)))
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
        let commit = build_entity_commit(stream, change_request_id)
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

    pub async fn find_change_requests_after<C: ConnectionTrait>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        commit_id: &CommitId,
        limit: u64,
    ) -> Result<Vec<CommittedChangeRequest>, anyhow::Error> {
        let commit_changes = Commits::find()
            .inner_join(ChangeRequests)
            .select_also(ChangeRequests)
            .filter(Column::UserTableId.eq(stream.user_table_id.val()))
            .filter(Column::StreamId.eq(stream.stream_id.val()))
            .filter(Column::Id.gt(commit_id.val()))
            .order_by_asc(Column::Id)
            .limit(limit)
            .all(conn)
            .await?;

        let mut change_requests = Vec::with_capacity(commit_changes.len());
        for (commit, change_request) in commit_changes {
            let change_request = change_request.ok_or(MangrobeError::UnexpectedState(
                // No entry should be None because the query uses INNER JOIN.
                "unexpected behavior. change_request is missing for commit".to_string(),
            ))?;

            let file_entry = ChangeRequestExt::build_domain_commited_file_entry(&change_request)?;
            change_requests.push(CommittedChangeRequest {
                commit_id: commit.id.into(),
                file_entry,
            });
        }

        Ok(change_requests)
    }

    pub async fn find_streams_after<C: ConnectionTrait>(
        &self,
        conn: &C,
        table_id: &UserTableId,
        stream_id: &Option<StreamId>,
        limit: u64,
    ) -> Result<Vec<Stream>, anyhow::Error> {
        let mut query = Commits::find()
            .select_only()
            .column(Column::StreamId)
            .column_as(Expr::col(Column::Id).max(), "last_commit_id")
            .filter(Column::UserTableId.eq(table_id.val()))
            .group_by(Column::StreamId);

        if let Some(stream_id) = stream_id {
            query = query.filter(Column::StreamId.gt(stream_id.val()));
        }

        let rows: Vec<(i64, i64)> = query
            .order_by_asc(Column::StreamId)
            .limit(limit)
            .into_tuple::<(i64, i64)>()
            .all(conn)
            .await?;

        let streams = rows
            .iter()
            .map(|(stream_id, last_commit_id)| Stream {
                id: stream_id.into(),
                last_commit_id: last_commit_id.into(),
            })
            .collect();

        Ok(streams)
    }
}
