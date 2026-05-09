use crate::application::data_definition::create_external_table_param::CreateExternalTableParam;
use crate::application::data_definition::create_table_param::CreateTableParam;
use crate::domain::model::table_definition::TableDefinition;
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
        let table_name = format!(
            "{}.{}.{}",
            param.table.identifier.catalog_name.val(),
            param.table.identifier.schema_name.val(),
            param.table.identifier.table_name.val()
        );
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
}
