use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use ahash::RandomState;
use sea_orm::DatabaseTransaction;
use sea_orm::ConnectionTrait;
use sea_orm::{DatabaseBackend, Statement};
use std::hash::{BuildHasher, Hasher};

// Kx values MUST NOT be changed. When they are changed, the number for advisory lock will be changed.
const K0: u64 = 0;
const K1: u64 = 1;
const K2: u64 = 2;
const K3: u64 = 3;

pub struct CommitLockRepository {
    hash_builder: RandomState,
}

impl CommitLockRepository {
    pub fn new() -> Self {
        let hash_builder = RandomState::with_seeds(K0, K1, K2, K3);
        Self { hash_builder }
    }

    // Acquire a lock that will be released automatically when its transaction ends.
    pub async fn acquire_xact_lock(
        &self,
        txn: &DatabaseTransaction,
        table_id: &UserTableId,
        tenant_id: &StreamId,
    ) -> Result<(), anyhow::Error> {
        txn.execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT pg_advisory_xact_lock($1)",
            [self.to_lock_id(table_id, tenant_id).into()],
        ))
        .await?;

        Ok(())
    }

    fn to_lock_id(&self, table_id: &UserTableId, tenant_id: &StreamId) -> i64 {
        let mut hasher = self.hash_builder.build_hasher();
        hasher.write_i64(tenant_id.val());
        hasher.write_i64(table_id.val());

        i64::from_ne_bytes(hasher.finish().to_ne_bytes())
    }
}
