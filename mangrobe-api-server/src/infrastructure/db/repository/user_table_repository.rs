use crate::domain::model::user_table::UserTable;
use crate::domain::model::user_table_name::UserTableName;
use crate::infrastructure::db::entity::prelude::UserTables;
use crate::infrastructure::db::entity::user_tables::{ActiveModel, Column};
use crate::infrastructure::db::repository::user_table_dto::build_domain_user_table;
use anyhow::bail;
use sea_orm::{ActiveValue::Set, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, SqlErr};
use thiserror::Error;

#[derive(Clone, Copy)]
pub struct UserTableRepository {}

#[derive(Error, Debug)]
pub enum UserTableRepositoryError {
    #[error("Already exists.")]
    AlreadyExists,
}

impl UserTableRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_by_name<C>(
        &self,
        conn: &C,
        name: &UserTableName,
    ) -> Result<Option<UserTable>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let table = UserTables::find()
            .filter(Column::Name.eq(name.val()))
            .one(conn)
            .await?;

        let Some(table) = table else {
            return Ok(None);
        };

        let table_dto = build_domain_user_table(&table)?;
        Ok(Some(table_dto))
    }

    pub async fn insert<C>(
        &self,
        conn: &C,
        name: &UserTableName,
    ) -> Result<UserTable, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let table = ActiveModel {
            id: Default::default(),
            name: Set(name.val()),
            created_at: Default::default(),
            updated_at: Default::default(),
        };

        let inserted = UserTables::insert(table).exec_with_returning(conn).await;
        match inserted {
            Ok(model) => Ok(UserTable::new(model.id.into(), name.clone())),
            Err(err) => {
                if self.is_unique_constraint_violation(&err) {
                    bail!(UserTableRepositoryError::AlreadyExists);
                }
                Err(err.into())
            }
        }
    }

    fn is_unique_constraint_violation(&self, err: &sea_orm::DbErr) -> bool {
        matches!(err.sql_err(), Some(SqlErr::UniqueConstraintViolation(_)))
    }
}
