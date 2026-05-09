use crate::application::data_definition::create_external_table_param::CreateExternalTableParam;
use crate::application::data_definition::create_table_param::CreateTableParam;
use crate::application::data_definition::get_table_param::GetTableParam;
use crate::application::data_definition::list_tables_param::ListTablesParam;
use crate::domain::model::table_definition::TableDefinition;
use crate::domain::model::table_summary::TableSummary;
use crate::domain::model::user_table::UserTable;
use crate::domain::service::user_table_service::UserTableService;
use crate::infrastructure::db::repository::user_table_repository::UserTableRepositoryError;
use crate::util::error::UserError;
use anyhow::bail;
use sea_orm::DatabaseConnection;

pub(crate) struct DataDefinitionUseCase {
    user_table_service: UserTableService,
}

impl DataDefinitionUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            user_table_service: UserTableService::new(&connection),
        }
    }

    pub async fn create_table(&self, param: CreateTableParam) -> Result<UserTable, anyhow::Error> {
        let res = self
            .user_table_service
            .create(&param.table_name, param.skip_if_exists)
            .await;

        match res {
            Ok(table) => Ok(table),
            Err(err) => {
                if let Some(e) = err.downcast_ref::<UserTableRepositoryError>() {
                    match e {
                        UserTableRepositoryError::AlreadyExists => {
                            bail!(UserError::AlreadyExistsMessage(param.table_name.val()));
                        }
                    }
                }
                bail!(err)
            }
        }
    }

    pub async fn create_external_table(
        &self,
        param: CreateExternalTableParam,
    ) -> Result<TableDefinition, anyhow::Error> {
        let table_name = param.table.identifier.full_name();
        let res = self
            .user_table_service
            .create_external_table(&param.table, param.skip_if_exists)
            .await;

        match res {
            Ok(table) => Ok(table),
            Err(err) => {
                if let Some(e) = err.downcast_ref::<UserTableRepositoryError>() {
                    match e {
                        UserTableRepositoryError::AlreadyExists => {
                            bail!(UserError::AlreadyExistsMessage(table_name));
                        }
                    }
                }
                bail!(err)
            }
        }
    }

    pub async fn get_table(&self, param: GetTableParam) -> Result<TableDefinition, anyhow::Error> {
        let table = self
            .user_table_service
            .find_by_identifier(&param.identifier)
            .await?;

        let Some(table) = table else {
            bail!(UserError::NotFoundMessage(param.identifier.full_name()));
        };

        Ok(table)
    }

    pub async fn list_tables(
        &self,
        param: ListTablesParam,
    ) -> Result<Vec<TableSummary>, anyhow::Error> {
        self.user_table_service
            .list_table_summaries(&param.catalog_name, &param.schema_name)
            .await
    }
}
