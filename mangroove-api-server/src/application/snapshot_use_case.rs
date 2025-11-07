use crate::domain::model::file::File;
use crate::domain::model::snapshot::Snapshot;
use crate::infrastructure::db::entity::prelude::{ChangeCommits, ChangeRequestFileAddEntries};
use crate::infrastructure::db::entity::{change_commits, change_request_file_add_entries};
use sea_orm::QueryFilter;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryOrder};

pub struct SnapshotUseCase {
    connection: DatabaseConnection,
}

impl SnapshotUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self { connection }
    }

    pub async fn get_snapshot(&self) -> Result<Snapshot, anyhow::Error> {
        // TODO: get "checkout" and changes after the checkout
        let commits = ChangeCommits::find()
            .order_by_desc(change_commits::Column::Id)
            .all(&self.connection)
            .await?;

        let change_request_ids: Vec<_> = commits.iter().map(|f| f.change_request_id).collect();

        let files = ChangeRequestFileAddEntries::find()
            .filter(
                change_request_file_add_entries::Column::ChangeRequestId.is_in(change_request_ids),
            )
            .all(&self.connection)
            .await?;

        let snapshot = Snapshot {
            files: files
                .iter()
                .map(|f| File {
                    path: f.path.clone(),
                    size: f.size,
                })
                .collect(),
        };
        Ok(snapshot)
    }
}
