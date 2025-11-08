use crate::domain::model::change_file_entry::ChangeFileEntry;
use crate::domain::model::change_request::{ChangeRequest, ChangeRequestType};
use crate::domain::model::change_request_id::ChangeRequestId;
use crate::infrastructure::db::entity::change_request_file_entries;
use crate::infrastructure::db::entity::prelude::ChangeRequestFileEntries;
use crate::infrastructure::db::entity_ext::change_request_ext::ChangeRequestExt;
use sea_orm::QueryFilter;
use sea_orm::{ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, Set};
use std::collections::HashMap;

#[derive(Clone)]
pub struct ChangeRequestFileEntryRepository {}

impl ChangeRequestFileEntryRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_all_by_change_request_ids<C>(
        &self,
        conn: &C,
        change_request_ids: &[ChangeRequestId],
    ) -> Result<HashMap<ChangeRequestId, Vec<ChangeFileEntry>>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let ids: Vec<i64> = change_request_ids.iter().map(|id| id.into()).collect();
        let entries = ChangeRequestFileEntries::find()
            .filter(change_request_file_entries::Column::ChangeRequestId.is_in(ids))
            .all(conn)
            .await?;

        let mut res = HashMap::<ChangeRequestId, Vec<ChangeFileEntry>>::new();
        for entry in entries {
            res.insert(
                entry.change_request_id.into(),
                serde_json::from_value(entry.change_entries.clone())?,
            );
        }

        Ok(res)
    }

    pub async fn insert(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        change_type: ChangeRequestType,
        target_entries: &Vec<ChangeFileEntry>,
    ) -> Result<(), anyhow::Error> {
        let change_entries = serde_json::to_value(target_entries)?;
        let entry = change_request_file_entries::ActiveModel {
            change_request_id: Set(change_request.id.i64()),
            change_type: Set(ChangeRequestExt::build_model_type(change_type)),
            change_entries: Set(change_entries),
            ..Default::default()
        };
        ChangeRequestFileEntries::insert(entry).exec(txn).await?;

        Ok(())
    }
}
