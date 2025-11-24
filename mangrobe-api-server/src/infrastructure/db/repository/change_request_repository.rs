use crate::domain::model::change_request::{
    ChangeRequest, ChangeRequestAbs, ChangeRequestStatus, ChangeRequestType,
};
use crate::domain::model::change_request_file_entry::ChangeRequestFileEntry::{
    AddFiles, ChangeFiles,
};
use crate::domain::model::change_request_file_entry::{
    ChangeRequestAddFilesEntry, ChangeRequestCompactFileEntry, ChangeRequestFileEntry,
};
use crate::domain::model::file_id::FileId;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::infrastructure::db::entity::change_request_idempotency_keys;
use crate::infrastructure::db::entity::change_requests::{ActiveModel, Column, Model};
use crate::infrastructure::db::entity::prelude::ChangeRequests;
use crate::infrastructure::db::entity_ext::change_request_ext::ChangeRequestExt;
use crate::infrastructure::db::repository::change_request_idempotency_key_repository::ChangeRequestIdempotencyKeyRepository;
use crate::util::error::MangrobeError;
use anyhow::{anyhow, bail};
use chrono::{DateTime, Utc};
use sea_orm::prelude::Expr;
use sea_orm::sea_query::LockType;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QuerySelect, Set,
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

    pub async fn find_or_create<C>(
        &self,
        conn: &C,
        idempotency_key: &IdempotencyKey,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        partition_time: &DateTime<Utc>,
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
                return self.build_domain_change_request(&change_request, &existing_key);
            }
            bail!("invalid state found. idempotency key doesn't belong any change requests");
        }

        let change_request = self
            .insert(conn, user_table_id, stream_id, partition_time, change_type)
            .await?;

        let inserted_key = self
            .idempotency_key_repository
            .insert_if_no_exists(
                conn,
                idempotency_key,
                &change_request.id.into(),
                Utc::now() + Duration::from_secs(24 * 3600 * 7),
            )
            .await?;
        if let Some(inserted_key) = inserted_key {
            return self.build_domain_change_request(&change_request, &inserted_key);
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
            return self.build_domain_change_request(&change_request, &existing_key);
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
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        partition_time: &DateTime<Utc>,
        change_type: ChangeRequestType,
    ) -> Result<Model, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let change_request =
            self.build_active_model(user_table_id, stream_id, partition_time, change_type);
        let res = change_request.insert(conn).await?;

        Ok(res)
    }

    fn build_active_model(
        &self,
        user_table_id: &UserTableId,
        stream_id: &StreamId,
        partition_time: &DateTime<Utc>,
        change_type: ChangeRequestType,
    ) -> ActiveModel {
        ActiveModel {
            stream_id: Set(stream_id.val()),
            user_table_id: Set(user_table_id.val()),
            partition_time: Set((*partition_time).into()),
            status: Set(ChangeRequestExt::build_model_status(
                ChangeRequestStatus::New,
            )),
            change_type: Set(ChangeRequestExt::build_model_change_type(change_type)),
            ..Default::default()
        }
    }

    fn build_domain_change_request(
        &self,
        change_request: &Model,
        idempotency_key: &change_request_idempotency_keys::Model,
    ) -> Result<ChangeRequest, anyhow::Error> {
        let idempotency_key = IdempotencyKey::try_from(idempotency_key.key.clone())
            .map_err(|_| anyhow!("invalid"))?;

        self.build_domain_change_request_with_idempotency_key(change_request, &idempotency_key)
    }

    fn build_domain_change_request_with_idempotency_key(
        &self,
        change_request: &Model,
        idempotency_key: &IdempotencyKey,
    ) -> Result<ChangeRequest, anyhow::Error> {
        let res = ChangeRequest {
            id: change_request.id.into(),
            idempotency_key: idempotency_key.clone(),
            user_table_id: change_request.user_table_id.into(),
            stream_id: change_request.stream_id.into(),
            partition_time: change_request.partition_time.to_utc(),
            status: ChangeRequestExt::build_domain_status(change_request)?,
            change_type: ChangeRequestExt::build_domain_change_type(change_request)?,
            file_entry: ChangeRequestExt::build_domain_file_entry(change_request)?,
        };

        Ok(res)
    }

    pub async fn select_for_update<CR>(
        &self,
        txn: &DatabaseTransaction,
        change_request: &CR,
    ) -> Result<ChangeRequest, anyhow::Error>
    where
        CR: ChangeRequestAbs,
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

        self.build_domain_change_request_with_idempotency_key(
            &selected,
            change_request.idempotency_key(),
        )
    }

    pub async fn update_status<CR>(
        &self,
        txn: &DatabaseTransaction,
        change_request: &CR,
        status: ChangeRequestStatus,
    ) -> Result<CR, anyhow::Error>
    where
        CR: ChangeRequestAbs,
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
        change_request: &CR,
        file_ids: &[FileId],
    ) -> Result<ChangeRequestFileEntry, anyhow::Error>
    where
        C: ConnectionTrait,
        CR: ChangeRequestAbs,
    {
        let entry = AddFiles {
            add_files: ChangeRequestAddFilesEntry {
                file_ids: Vec::from(file_ids),
            },
        };

        self.update_file_entry(conn, change_request, &entry).await?;

        Ok(entry)
    }

    pub async fn update_change_file_entry<C, CR>(
        &self,
        conn: &C,
        change_request: &CR,
        add_file_ids: &[FileId],
    ) -> Result<ChangeRequestFileEntry, anyhow::Error>
    where
        C: ConnectionTrait,
        CR: ChangeRequestAbs,
    {
        let entry = ChangeFiles {
            add_files: ChangeRequestAddFilesEntry {
                file_ids: Vec::from(add_file_ids),
            },
        };

        self.update_file_entry(conn, change_request, &entry).await?;

        Ok(entry)
    }

    pub async fn update_compact_file_entry<C, CR>(
        &self,
        conn: &C,
        change_request: &CR,
        src_file_ids: &[FileId],
        dst_file_id: &FileId,
    ) -> Result<ChangeRequestFileEntry, anyhow::Error>
    where
        C: ConnectionTrait,
        CR: ChangeRequestAbs,
    {
        let entry = ChangeRequestFileEntry::Compact {
            compact: ChangeRequestCompactFileEntry {
                src_file_ids: Vec::from(src_file_ids),
                dst_file_id: dst_file_id.clone(),
            },
        };

        self.update_file_entry(conn, change_request, &entry).await?;

        Ok(entry)
    }

    async fn update_file_entry<C, CR>(
        &self,
        conn: &C,
        change_request: &CR,
        file_entry: &ChangeRequestFileEntry,
    ) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
        CR: ChangeRequestAbs,
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
