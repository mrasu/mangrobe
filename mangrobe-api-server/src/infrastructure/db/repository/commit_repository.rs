use crate::domain::model::commit_id::CommitId;
use crate::domain::model::change_request_id::ChangeRequestId;
use crate::util::error::MangobeError;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QueryOrder, Set,
};
use crate::infrastructure::db::entity::commits;
use crate::infrastructure::db::entity::prelude::Commits;

pub struct CommitRepository {}

impl CommitRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn fetch_change_request_ids_for_latest<C>(
        &self,
        conn: &C,
    ) -> Result<Vec<ChangeRequestId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let commits = Commits::find()
            .order_by_desc(commits::Column::Id)
            .all(conn)
            .await?;

        let ids = commits
            .iter()
            .map(|c| ChangeRequestId::from(c.change_request_id))
            .collect();

        Ok(ids)
    }

    pub async fn insert<C>(
        &self,
        con: &C,
        change_request_id: ChangeRequestId,
    ) -> Result<CommitId, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let commit = commits::ActiveModel {
            change_request_id: Set(change_request_id.into()),
            ..Default::default()
        }
        .insert(con)
        .await?;

        Ok(commit.id.into())
    }

    pub async fn get_by_change_request_id(
        &self,
        txn: &DatabaseTransaction,
        change_request_id: ChangeRequestId,
    ) -> Result<CommitId, anyhow::Error> {
        let commit = Commits::find()
            .filter(commits::Column::ChangeRequestId.eq(change_request_id.i64()))
            .one(txn)
            .await?
            .ok_or(MangobeError::UnexpectedState(
                "no commit found but marked as commited.".to_string(),
            ))?;

        Ok(commit.id.into())
    }
}
