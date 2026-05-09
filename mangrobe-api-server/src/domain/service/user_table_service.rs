use crate::domain::model::table_definition::TableDefinition;
use crate::domain::model::user_table::UserTable;
use crate::domain::model::user_table_id::UserTableId;
use crate::domain::model::user_table_name::UserTableName;
use crate::infrastructure::db::repository::user_table_repository::UserTableRepository;
use sea_orm::DatabaseConnection;

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

    pub async fn create(
        &self,
        name: &UserTableName,
        skip_if_exists: bool,
    ) -> Result<UserTable, anyhow::Error> {
        if skip_if_exists {
            let table = self
                .user_table_repository
                .find_by_name(&self.connection, name)
                .await?;
            if let Some(table) = table {
                return Ok(table);
            }
        }

        let table = self
            .user_table_repository
            .insert(&self.connection, name)
            .await?;

        Ok(table)
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

    pub async fn find_id_by_name(
        &self,
        name: &UserTableName,
    ) -> Result<Option<UserTableId>, anyhow::Error> {
        let table = self
            .user_table_repository
            .find_by_name(&self.connection, name)
            .await?;

        let Some(table) = table else { return Ok(None) };

        Ok(Some(table.id))
    }
}
