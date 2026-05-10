use crate::domain::model::table_definition::{Column, TableDefinition};
use crate::domain::model::table_identifier::TableIdentifier;
use crate::domain::model::table_summary::TableSummary;
use crate::domain::model::user_table_id::UserTableId;
use crate::infrastructure::db::repository::user_table_repository::UserTableRepository;
use crate::util::error::UserError;
use anyhow::anyhow;
use sea_orm::{DatabaseConnection, TransactionTrait};

pub(crate) struct UserTableService {
    connection: DatabaseConnection,
    user_table_repository: UserTableRepository,
}

impl UserTableService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            user_table_repository: UserTableRepository::new(),
        }
    }

    pub async fn create_external_table(
        &self,
        table: &TableDefinition,
        skip_if_exists: bool,
    ) -> Result<TableDefinition, anyhow::Error> {
        if skip_if_exists {
            let existing = self
                .user_table_repository
                .find_by_identifier(&self.connection, &table.identifier)
                .await?;
            if let Some(existing) = existing {
                return Ok(existing);
            }
        }

        self.user_table_repository
            .insert_table_definition(&self.connection, table)
            .await
    }

    pub async fn find_id_by_identifier(
        &self,
        identifier: &TableIdentifier,
    ) -> Result<Option<UserTableId>, anyhow::Error> {
        let id = self
            .user_table_repository
            .find_id_by_identifier(&self.connection, identifier)
            .await?;

        let Some(id) = id else { return Ok(None) };

        Ok(Some(id))
    }

    pub async fn find_by_identifier(
        &self,
        identifier: &TableIdentifier,
    ) -> Result<Option<TableDefinition>, anyhow::Error> {
        self.user_table_repository
            .find_by_identifier(&self.connection, identifier)
            .await
    }

    pub async fn list_table_summaries(
        &self,
        catalog_name: &Option<String>,
        schema_name: &Option<String>,
    ) -> Result<Vec<TableSummary>, anyhow::Error> {
        self.user_table_repository
            .find_table_summaries(&self.connection, catalog_name, schema_name)
            .await
    }

    pub async fn evolve_schema(
        &self,
        identifier: &TableIdentifier,
        proposed_columns: Vec<Column>,
    ) -> Result<Option<TableDefinition>, anyhow::Error> {
        let txn = self.connection.begin().await?;

        let Some(table) = self
            .user_table_repository
            .find_by_identifier_for_update(&txn, identifier)
            .await?
        else {
            txn.rollback().await?;
            return Ok(None);
        };

        let (columns, changed) = table
            .evolve_schema_columns(proposed_columns)
            .map_err(|err| anyhow!(UserError::InvalidParameterMessage(err.to_string())))?;

        if !changed {
            txn.rollback().await?;
            return Ok(Some(table));
        }

        let table = self
            .user_table_repository
            .update_columns(&txn, identifier, &columns)
            .await?;

        txn.commit().await?;

        Ok(Some(table))
    }
}
