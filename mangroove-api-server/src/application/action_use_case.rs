use crate::domain::change_log::ChangedFiles;
use crate::domain::change_log_id::ChangeLogId;
use crate::infrastructure::db::entity::change_requests;
use crate::infrastructure::db::entity::prelude::ChangeRequestAddFiles;
use crate::infrastructure::db::entity::{change_commits, change_request_add_files};
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, Set};

pub struct ActionUseCase {
    connection: DatabaseConnection,
}

impl ActionUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self { connection }
    }

    pub async fn change_files(
        &self,
        idempotency_key: String,
        changed_files: &ChangedFiles,
    ) -> Result<ChangeLogId, Box<dyn std::error::Error>> {
        // TODO: check idempotency
        let change_request = change_requests::ActiveModel {
            idempotency_key: Set(idempotency_key),
            ..Default::default()
        };
        let change_request = change_request.insert(&self.connection).await?;

        let changed_files =
            changed_files
                .added_files
                .iter()
                .map(|file| change_request_add_files::ActiveModel {
                    change_request_id: Set(change_request.id),
                    path: Set(file.path.clone()),
                    size: Set(file.size),
                    ..Default::default()
                });
        ChangeRequestAddFiles::insert_many(changed_files)
            .exec(&self.connection)
            .await?;

        // TODO: more wise when adding compaction or expiration

        // TODO: lock to not race with other change.
        let commit = change_commits::ActiveModel {
            change_request_id: Set(change_request.id),
            ..Default::default()
        }
        .insert(&self.connection)
        .await?;

        Ok(ChangeLogId::from(commit.id))
    }
}
