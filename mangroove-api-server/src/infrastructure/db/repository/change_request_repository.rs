use crate::domain::model::change_request::{ChangeRequest, ChangeRequestStatus};
use crate::infrastructure::db::entity::prelude::{ChangeRequestIdempotencyKeys, ChangeRequests};
use crate::infrastructure::db::entity::{change_request_idempotency_keys, change_requests};
use crate::infrastructure::db::entity_ext::change_request_ext::ChangeRequestExt;
use crate::util::error::MangobeError;
use anyhow::bail;
use chrono::{DateTime, Utc};
use sea_orm::prelude::Expr;
use sea_orm::sea_query::LockType;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction,
    EntityTrait, QueryFilter, QuerySelect, Set, TryInsertResult,
};
use std::time::Duration;

pub struct ChangeRequestRepository {}

impl ChangeRequestRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_or_create<C>(
        &self,
        con: &C,
        idempotency_key: Vec<u8>,
        tenant_id: i64,
        partition_time: DateTime<Utc>,
    ) -> Result<ChangeRequest, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let existing_key = ChangeRequestIdempotencyKeys::find()
            .filter(change_request_idempotency_keys::Column::Key.eq(idempotency_key.clone()))
            .one(con)
            .await?;

        if let Some(existing_key) = existing_key {
            let change_request = ChangeRequests::find()
                .filter(change_requests::Column::Id.eq(existing_key.change_request_id))
                .one(con)
                .await?;
            if let Some(change_request) = change_request {
                return Ok(Self::build_domain_change_request(
                    &change_request,
                    &existing_key,
                )?);
            }
            bail!("invalid state found. idempotency key doesn't belong any change requests");
        }

        let change_request = change_requests::ActiveModel {
            tenant_id: Set(tenant_id),
            partition_time: Set(partition_time.into()),
            status: Set(ChangeRequestExt::build_model_status(
                ChangeRequestStatus::New,
            )),
            ..Default::default()
        };
        let change_request = change_request.insert(con).await?;

        let new_idempotency_key = change_request_idempotency_keys::ActiveModel {
            change_request_id: Set(change_request.id),
            key: Set(idempotency_key.clone()),
            expires_at: Set((Utc::now() + Duration::from_secs(24 * 3600 * 7)).into()),
            ..Default::default()
        };
        let result = change_request_idempotency_keys::Entity::insert(new_idempotency_key)
            .on_conflict_do_nothing()
            .exec_with_returning(con)
            .await?;
        if let TryInsertResult::Inserted(inserted) = result {
            return Ok(Self::build_domain_change_request(
                &change_request,
                &inserted,
            )?);
        }

        let existing_key = ChangeRequestIdempotencyKeys::find()
            .filter(change_request_idempotency_keys::Column::Key.eq(idempotency_key.clone()))
            .one(con)
            .await?;
        let Some(existing_key) = existing_key else {
            bail!("invalid situation. cannot add idempotency key but no change request for it",);
        };

        let change_request = ChangeRequests::find()
            .filter(change_requests::Column::Id.eq(existing_key.change_request_id))
            .one(con)
            .await?;
        if let Some(change_request) = change_request {
            return Ok(Self::build_domain_change_request(
                &change_request,
                &existing_key,
            )?);
        }

        bail!("invalid state found. idempotency key doesn't belong any change requests now")
    }

    fn build_domain_change_request(
        change_request: &change_requests::Model,
        idempotency_key: &change_request_idempotency_keys::Model,
    ) -> Result<ChangeRequest, anyhow::Error> {
        let res = ChangeRequest {
            id: change_request.id,
            idempotency_key: idempotency_key.key.clone(),
            tenant_id: change_request.tenant_id,
            partition_time: change_request.partition_time.to_utc(),
            status: ChangeRequestExt::build_domain_status(change_request)?,
        };

        Ok(res)
    }

    pub async fn select_for_update(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
    ) -> Result<ChangeRequestStatus, anyhow::Error> {
        let result = ChangeRequests::find()
            .filter(change_requests::Column::Id.eq(change_request.id))
            .lock(LockType::Update)
            .one(txn)
            .await?;

        let Some(selected) = result else {
            bail!(MangobeError::UnexpectedState(
                "ChangeRequests disappeared".into()
            ));
        };

        ChangeRequestExt::build_domain_status(&selected)
    }

    pub async fn update_status(
        &self,
        txn: &DatabaseTransaction,
        change_request: &ChangeRequest,
        status: ChangeRequestStatus,
    ) -> Result<(), anyhow::Error> {
        ChangeRequests::update_many()
            .filter(change_requests::Column::Id.eq(change_request.id))
            .col_expr(
                change_requests::Column::Status,
                Expr::value(ChangeRequestExt::build_model_status(status)),
            )
            .exec(txn)
            .await?;

        Ok(())
    }
}
