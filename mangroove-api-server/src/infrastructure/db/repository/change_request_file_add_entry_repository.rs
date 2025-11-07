use crate::domain::model::change_log::ChangeRequestFileAddEntry;
use crate::domain::model::change_request::ChangeRequest;
use crate::domain::model::change_request_id::ChangeRequestId;
use crate::domain::model::file::File;
use crate::infrastructure::db::entity::change_request_file_add_entries;
use crate::infrastructure::db::entity::prelude::ChangeRequestFileAddEntries;
use sea_orm::QueryFilter;
use sea_orm::{ColumnTrait, ConnectionTrait, DatabaseTransaction, DbErr, EntityTrait, Set};

#[derive(Clone)]
pub struct ChangeRequestFileAddEntryRepository {}

impl ChangeRequestFileAddEntryRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_by_change_request_ids<C>(
        &self,
        conn: &C,
        change_request_ids: Vec<ChangeRequestId>,
    ) -> Result<Vec<File>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let ids: Vec<i64> = change_request_ids.iter().map(|id| id.into()).collect();
        let files = ChangeRequestFileAddEntries::find()
            .filter(change_request_file_add_entries::Column::ChangeRequestId.is_in(ids))
            .all(conn)
            .await?;

        let res = files
            .iter()
            .map(|f| File {
                path: f.path.clone(),
                size: f.size,
            })
            .collect();
        Ok(res)
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
                    change_request_id: Set(change_request.id.i64()),
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
