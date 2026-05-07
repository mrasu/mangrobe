use crate::api::core::data_definition::create_table_param::build_create_table_param;
use crate::api::core::data_definition::create_table_response::build_create_table_response;
use crate::api::grpc::proto::{CreateTableRequest, CreateTableResponse};
use crate::application::data_definition::data_definition_use_case::DataDefinitionUseCase;
use sea_orm::DatabaseConnection;

pub struct DataDefinitionService {
    data_definition_use_case: DataDefinitionUseCase,
}

impl DataDefinitionService {
    pub(crate) fn new(db: DatabaseConnection) -> Self {
        Self {
            data_definition_use_case: DataDefinitionUseCase::new(db),
        }
    }

    pub async fn create_table(
        &self,
        param: CreateTableRequest,
    ) -> Result<CreateTableResponse, anyhow::Error> {
        let param = build_create_table_param(&param)?;

        let table = self.data_definition_use_case.create_table(param).await?;

        Ok(build_create_table_response(table))
    }
}
