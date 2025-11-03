use crate::domain::file::File;
use crate::domain::snapshot::Snapshot;
use crate::infrastructure::db::entity::delta_logs;
use crate::infrastructure::db::entity::prelude::{DeltaFiles, DeltaLogs};
use sea_orm::{DatabaseConnection, EntityTrait, LoaderTrait, QueryOrder};

pub struct SnapshotUseCase {
    connection: DatabaseConnection,
}

impl SnapshotUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self { connection }
    }

    pub async fn get_snapshot(&self) -> Result<Snapshot, Box<dyn std::error::Error>> {
        // TODO: get "checkout" and delta_logs after the checkout
        let checkout = DeltaLogs::find()
            .order_by_desc(delta_logs::Column::Id)
            .all(&self.connection)
            .await?;

        let files = checkout.load_many(DeltaFiles, &self.connection).await?;

        let snapshot = Snapshot {
            files: files
                .iter()
                .flatten()
                .map(|f| File {
                    path: f.path.clone(),
                    size: f.size,
                })
                .collect(),
        };
        Ok(snapshot)
    }
}
