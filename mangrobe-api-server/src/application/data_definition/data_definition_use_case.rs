use crate::application::data_definition::CreateTableParam;
use crate::domain::model::user_table::UserTable;
use crate::domain::service::user_table_service::UserTableService;
use crate::infrastructure::db::repository::user_table_repository::UserTableRepositoryError;
use crate::util::error::UserError;
use anyhow::bail;
use sea_orm::DatabaseConnection;

pub struct DataDefinitionUseCase {
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
}
