use crate::domain::model::table_definition::{Column as DomainColumn, TableDefinition};
use crate::domain::model::table_identifier::TableIdentifier;
use crate::domain::model::table_summary::TableSummary;
use crate::domain::model::user_table_id::UserTableId;
use crate::infrastructure::db::entity::prelude::UserTables;
use crate::infrastructure::db::entity::user_tables::Column;
use crate::infrastructure::db::repository::user_table_dto::{
    build_active_model, build_columns_value, build_domain_table_definition,
    build_domain_table_summary,
};
use anyhow::bail;
use sea_orm::sea_query::LockType;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, SqlErr,
};
use thiserror::Error;

#[derive(Clone, Copy)]
pub(crate) struct UserTableRepository {}

#[derive(Error, Debug)]
pub(crate) enum UserTableRepositoryError {
    #[error("Already exists.")]
    AlreadyExists,

    #[error("Not found.")]
    NotFound,
}

impl UserTableRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_id_by_identifier<C>(
        &self,
        conn: &C,
        identifier: &TableIdentifier,
    ) -> Result<Option<UserTableId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let table = UserTables::find()
            .filter(Column::CatalogName.eq(identifier.catalog_name.val()))
            .filter(Column::SchemaName.eq(identifier.schema_name.val()))
            .filter(Column::Name.eq(identifier.table_name.val()))
            .column(Column::Id)
            .into_tuple::<i64>()
            .one(conn)
            .await?;

        let Some(id) = table else {
            return Ok(None);
        };

        Ok(Some(id.into()))
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

        Ok(Some(build_domain_table_definition(&table)?))
    }

    pub async fn find_by_identifier_for_update(
        &self,
        txn: &DatabaseTransaction,
        identifier: &TableIdentifier,
    ) -> Result<Option<TableDefinition>, anyhow::Error> {
        let table = UserTables::find()
            .filter(Column::CatalogName.eq(identifier.catalog_name.val()))
            .filter(Column::SchemaName.eq(identifier.schema_name.val()))
            .filter(Column::Name.eq(identifier.table_name.val()))
            .lock(LockType::Update)
            .one(txn)
            .await?;

        let Some(table) = table else {
            return Ok(None);
        };

        Ok(Some(build_domain_table_definition(&table)?))
    }

    pub async fn find_table_summaries<C>(
        &self,
        conn: &C,
        catalog_name: &Option<String>,
        schema_name: &Option<String>,
    ) -> Result<Vec<TableSummary>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let mut query = UserTables::find()
            .select_only()
            .column(Column::CatalogName)
            .column(Column::SchemaName)
            .column(Column::Name)
            .column(Column::Comment);

        if let Some(catalog_name) = catalog_name {
            query = query.filter(Column::CatalogName.eq(catalog_name));
        }
        if let Some(schema_name) = schema_name {
            query = query.filter(Column::SchemaName.eq(schema_name));
        }

        let rows: Vec<(String, String, String, Option<String>)> = query
            .order_by_asc(Column::CatalogName)
            .order_by_asc(Column::SchemaName)
            .order_by_asc(Column::Name)
            .into_tuple::<(String, String, String, Option<String>)>()
            .all(conn)
            .await?;

        rows.into_iter()
            .map(|(catalog_name, schema_name, table_name, comment)| {
                build_domain_table_summary(catalog_name, schema_name, table_name, comment)
            })
            .collect()
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
            Ok(model) => build_domain_table_definition(&model),
            Err(err) => {
                if self.is_unique_constraint_violation(&err) {
                    bail!(UserTableRepositoryError::AlreadyExists);
                }
                Err(err.into())
            }
        }
    }

    pub async fn update_columns<C>(
        &self,
        conn: &C,
        identifier: &TableIdentifier,
        columns: &[DomainColumn],
    ) -> Result<TableDefinition, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let result = UserTables::update_many()
            .filter(Column::CatalogName.eq(identifier.catalog_name.val()))
            .filter(Column::SchemaName.eq(identifier.schema_name.val()))
            .filter(Column::Name.eq(identifier.table_name.val()))
            .col_expr(Column::Columns, build_columns_value(columns).into())
            .exec(conn)
            .await?;

        if result.rows_affected == 0 {
            bail!(UserTableRepositoryError::NotFound);
        }

        let Some(table) = self.find_by_identifier(conn, identifier).await? else {
            bail!(UserTableRepositoryError::NotFound);
        };

        Ok(table)
    }

    fn is_unique_constraint_violation(&self, err: &sea_orm::DbErr) -> bool {
        matches!(err.sql_err(), Some(SqlErr::UniqueConstraintViolation(_)))
    }
}
