use crate::domain::model::table_definition::{TableDefinition, TableIdentifier};
use crate::domain::model::user_table::UserTable;
use crate::domain::model::user_table_name::UserTableName;
use crate::infrastructure::db::entity::prelude::UserTables;
use crate::infrastructure::db::entity::user_tables::{ActiveModel, Column};
use crate::infrastructure::db::repository::user_table_dto::{
    build_active_model, build_domain_user_table, build_table_definition,
};
use anyhow::bail;
use sea_orm::{ActiveValue::Set, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, SqlErr};
use serde_json::json;
use thiserror::Error;

#[derive(Clone, Copy)]
pub(crate) struct UserTableRepository {}

#[derive(Error, Debug)]
pub(crate) enum UserTableRepositoryError {
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
            catalog_name: Set("default".to_owned()),
            schema_name: Set("default".to_owned()),
            name: Set(name.val()),
            location: Set(json!({})),
            format: Set(0),
            columns: Set(json!([])),
            partitions: Set(json!([])),
            comment: Set(None),
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

    pub async fn find_by_identifier<C>(
        &self,
        conn: &C,
        identifier: &TableIdentifier,
    ) -> Result<Option<TableDefinition>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let table = UserTables::find()
            .filter(Column::CatalogName.eq(identifier.catalog_name.val()))
            .filter(Column::SchemaName.eq(identifier.schema_name.val()))
            .filter(Column::Name.eq(identifier.table_name.val()))
            .one(conn)
            .await?;

        let Some(table) = table else {
            return Ok(None);
        };

        Ok(Some(build_table_definition(&table)?))
    }

    pub async fn insert_table_definition<C>(
        &self,
        conn: &C,
        table: &TableDefinition,
    ) -> Result<TableDefinition, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let inserted = UserTables::insert(build_active_model(table))
            .exec_with_returning(conn)
            .await;
        match inserted {
            Ok(model) => build_table_definition(&model),
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
