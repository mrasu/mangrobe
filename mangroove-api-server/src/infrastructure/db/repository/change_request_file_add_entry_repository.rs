use crate::domain::model::change_log::ChangeRequestFileAddEntry;
use crate::domain::model::change_request::ChangeRequest;
use crate::infrastructure::db::entity::change_request_file_add_entries;
use crate::infrastructure::db::entity::prelude::ChangeRequestFileAddEntries;
use sea_orm::{DatabaseTransaction, DbErr, EntityTrait, Set};

#[derive(Clone)]
pub struct ChangeRequestFileAddEntryRepository {}

impl ChangeRequestFileAddEntryRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn insert(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        target_entries: &Vec<ChangeRequestFileAddEntry>,
    ) -> Result<(), DbErr> {
        let entries =
            target_entries
                .iter()
                .map(|file| change_request_file_add_entries::ActiveModel {
                    change_request_id: Set(change_request.id),
                    path: Set(file.path.clone()),
                    size: Set(file.size),
                    ..Default::default()
                });
        println!("{:?}", entries);
        ChangeRequestFileAddEntries::insert_many(entries)
            .exec(txn)
            .await?;

        Ok(())
    }
}
