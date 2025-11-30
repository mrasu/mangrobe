use crate::domain::model::change_request::{
    ChangeRequest, ChangeRequestStatus, ChangeRequestTrait, ChangeRequestType,
};
use crate::domain::model::change_request_file_entry::ChangeRequestFileEntry::{
    AddFiles, ChangeFiles,
};
use crate::domain::model::change_request_file_entry::{
    ChangeRequestAddFilesEntry, ChangeRequestChangeFilesEntry, ChangeRequestCompactFileEntry,
    ChangeRequestCompactFilesEntry, ChangeRequestFileEntry,
};
use crate::domain::model::file_id::FileId;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::change_requests::{Column, Model};
use crate::infrastructure::db::entity::prelude::ChangeRequests;
use crate::infrastructure::db::entity_ext::change_request_ext::ChangeRequestExt;
use crate::infrastructure::db::repository::change_request_dto::{
    build_domain_change_request, build_entity_change_request,
};
use crate::infrastructure::db::repository::change_request_idempotency_key_repository::ChangeRequestIdempotencyKeyRepository;
use crate::util::error::MangrobeError;
use anyhow::bail;
use chrono::Utc;
use sea_orm::prelude::Expr;
use sea_orm::sea_query::LockType;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QuerySelect,
};
use std::time::Duration;

pub struct ChangeRequestRepository {
    idempotency_key_repository: ChangeRequestIdempotencyKeyRepository,
}

impl ChangeRequestRepository {
    pub fn new() -> Self {
        Self {
            idempotency_key_repository: ChangeRequestIdempotencyKeyRepository::new(),
        }
    }

    pub async fn create<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        change_type: ChangeRequestType,
    ) -> Result<ChangeRequest, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let change_request = self.insert(conn, stream, change_type).await?;
        build_domain_change_request(&change_request)
    }

    pub async fn find_by_idempotency_key_or_create<C>(
        &self,
        conn: &C,
        idempotency_key: &IdempotencyKey,
        stream: &UserTablStream,
        change_type: ChangeRequestType,
    ) -> Result<ChangeRequest, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let existing_key = self
            .idempotency_key_repository
            .find_by_key(conn, idempotency_key)
            .await?;
        if let Some(existing_key) = existing_key {
            let change_request = self
                .find_by_id(conn, existing_key.change_request_id)
                .await?;
            if let Some(change_request) = change_request {
                return build_domain_change_request(&change_request);
            }
            bail!("invalid state found. idempotency key doesn't belong any change requests");
        }

        let change_request = self.insert(conn, stream, change_type).await?;

        let inserted_key = self
            .idempotency_key_repository
            .insert_if_no_exists(
                conn,
                idempotency_key,
                &change_request.id.into(),
                Utc::now() + Duration::from_secs(24 * 3600 * 7),
            )
            .await?;
        if inserted_key.is_some() {
            return build_domain_change_request(&change_request);
        }

        let existing_key = self
            .idempotency_key_repository
            .find_by_key(conn, idempotency_key)
            .await?;
        let Some(existing_key) = existing_key else {
            bail!("invalid situation. cannot add idempotency key but no change request for it",);
        };

        let change_request = self
            .find_by_id(conn, existing_key.change_request_id)
            .await?;
        if let Some(change_request) = change_request {
            return build_domain_change_request(&change_request);
        }

        bail!("invalid state found. idempotency key doesn't belong any change requests now")
    }

    async fn find_by_id<C>(&self, conn: &C, id: i64) -> Result<Option<Model>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let res = ChangeRequests::find()
            .filter(Column::Id.eq(id))
            .one(conn)
            .await?;

        Ok(res)
    }

    async fn insert<C>(
        &self,
        conn: &C,
        stream: &UserTablStream,
        change_type: ChangeRequestType,
    ) -> Result<Model, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let change_request = build_entity_change_request(stream, change_type);
        let res = change_request.insert(conn).await?;

        Ok(res)
    }

    pub async fn select_for_update<CR>(
        &self,
        txn: &DatabaseTransaction,
        change_request: &CR,
    ) -> Result<ChangeRequest, anyhow::Error>
    where
        CR: ChangeRequestTrait,
    {
        let result = ChangeRequests::find()
            .filter(Column::Id.eq(change_request.id().val()))
            .lock(LockType::Update)
            .one(txn)
            .await?;

        let Some(selected) = result else {
            bail!(MangrobeError::UnexpectedState(
                "ChangeRequests disappeared".into()
            ));
        };

        build_domain_change_request(&selected)
    }

    pub async fn update_status<CR>(
        &self,
        txn: &DatabaseTransaction,
        change_request: CR,
        status: ChangeRequestStatus,
    ) -> Result<CR, anyhow::Error>
    where
        CR: ChangeRequestTrait,
    {
        ChangeRequests::update_many()
            .filter(Column::Id.eq(change_request.id().val()))
            .col_expr(
                Column::Status,
                Expr::value(ChangeRequestExt::build_model_status(status)),
            )
            .exec(txn)
            .await?;

        Ok(change_request.set_status(status))
    }

    pub async fn update_add_file_entry<C, CR>(
        &self,
        conn: &C,
        change_request: CR,
        file_ids: &[FileId],
    ) -> Result<ChangeRequestAddFilesEntry, anyhow::Error>
    where
        C: ConnectionTrait,
        CR: ChangeRequestTrait,
    {
        let add_files_entry = ChangeRequestAddFilesEntry {
            file_ids: Vec::from(file_ids),
        };

        let entry = AddFiles {
            add_files: add_files_entry.clone(),
        };

        self.update_file_entry(conn, change_request, &entry).await?;

        Ok(add_files_entry)
    }

    pub async fn update_change_file_entry<C, CR>(
        &self,
        conn: &C,
        change_request: CR,
        delete_file_ids: &[FileId],
    ) -> Result<ChangeRequestChangeFilesEntry, anyhow::Error>
    where
        C: ConnectionTrait,
        CR: ChangeRequestTrait,
    {
        let change_files_entry = ChangeRequestChangeFilesEntry {
            delete_file_ids: Vec::from(delete_file_ids),
        };

        let entry = ChangeFiles {
            change_files: change_files_entry.clone(),
        };

        self.update_file_entry(conn, change_request, &entry).await?;

        Ok(change_files_entry)
    }

    pub async fn update_compact_file_entry<C, CR>(
        &self,
        conn: &C,
        change_request: CR,
        compact_entries: &[ChangeRequestCompactFileEntry],
    ) -> Result<ChangeRequestCompactFilesEntry, anyhow::Error>
    where
        C: ConnectionTrait,
        CR: ChangeRequestTrait,
    {
        let compact_files_entry = ChangeRequestCompactFilesEntry {
            entries: Vec::from(compact_entries),
        };
        let entry = ChangeRequestFileEntry::Compact {
            compact: compact_files_entry.clone(),
        };

        self.update_file_entry(conn, change_request, &entry).await?;

        Ok(compact_files_entry)
    }

    async fn update_file_entry<C, CR>(
        &self,
        conn: &C,
        change_request: CR,
        file_entry: &ChangeRequestFileEntry,
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
        CR: ChangeRequestTrait,
    {
        let entry_json = ChangeRequestExt::build_model_file_entry(file_entry)?;
        ChangeRequests::update_many()
            .col_expr(Column::FileEntry, entry_json.into())
            .filter(Column::Id.eq(change_request.id().val()))
            .exec(conn)
            .await?;

        Ok(())
    }
}
