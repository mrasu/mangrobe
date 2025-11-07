use crate::domain::model::change_log_id::ChangeLogId;
use crate::domain::model::change_request_id::ChangeRequestId;
use crate::infrastructure::db::entity::change_commits;
use crate::infrastructure::db::entity::prelude::ChangeCommits;
use crate::util::error::MangobeError;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QueryOrder, Set,
};

pub struct ChangeCommitRepository {}

impl ChangeCommitRepository {
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
        let commits = ChangeCommits::find()
            .order_by_desc(change_commits::Column::Id)
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
    ) -> Result<ChangeLogId, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let commit = change_commits::ActiveModel {
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
    ) -> Result<ChangeLogId, anyhow::Error> {
        let commit = ChangeCommits::find()
            .filter(change_commits::Column::ChangeRequestId.eq(change_request_id.i64()))
            .one(txn)
            .await?
            .ok_or(MangobeError::UnexpectedState(
                "no commit found but marked as commited.".to_string(),
            ))?;

        Ok(commit.id.into())
    }
}
