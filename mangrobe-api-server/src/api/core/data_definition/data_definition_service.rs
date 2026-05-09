use crate::api::core::data_definition::create_external_table_param::build_create_external_table_param;
use crate::api::core::data_definition::create_external_table_response::build_create_external_table_response;
use crate::api::core::data_definition::create_table_param::build_create_table_param;
use crate::api::core::data_definition::create_table_response::build_create_table_response;
use crate::api::core::data_definition::evolve_table_schema_param::build_evolve_table_schema_param;
use crate::api::core::data_definition::evolve_table_schema_response::build_evolve_table_schema_response;
use crate::api::core::data_definition::get_table_param::build_get_table_param;
use crate::api::core::data_definition::get_table_response::build_get_table_response;
use crate::api::core::data_definition::list_tables_param::parse_list_tables_param;
use crate::api::core::data_definition::list_tables_response::build_list_tables_response;
use crate::api::grpc::proto::{
    CreateExternalTableRequest, CreateExternalTableResponse, CreateTableRequest,
    CreateTableResponse, EvolveTableSchemaRequest, EvolveTableSchemaResponse, GetTableRequest,
    GetTableResponse, ListTablesRequest, ListTablesResponse,
};
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

    pub async fn create_external_table(
        &self,
        param: CreateExternalTableRequest,
    ) -> Result<CreateExternalTableResponse, anyhow::Error> {
        let param = build_create_external_table_param(&param)?;

        let table = self
            .data_definition_use_case
            .create_external_table(param)
            .await?;

        Ok(build_create_external_table_response(table))
    }

    pub async fn get_table(
        &self,
        param: GetTableRequest,
    ) -> Result<GetTableResponse, anyhow::Error> {
        let param = build_get_table_param(&param)?;

        let table = self.data_definition_use_case.get_table(param).await?;

        Ok(build_get_table_response(table))
    }

    pub async fn list_tables(
        &self,
        param: ListTablesRequest,
    ) -> Result<ListTablesResponse, anyhow::Error> {
        let param = parse_list_tables_param(&param);

        let tables = self.data_definition_use_case.list_tables(param).await?;

        Ok(build_list_tables_response(tables))
    }

    pub async fn evolve_table_schema(
        &self,
        param: EvolveTableSchemaRequest,
    ) -> Result<EvolveTableSchemaResponse, anyhow::Error> {
        let param = build_evolve_table_schema_param(&param)?;

        let table = self
            .data_definition_use_case
            .evolve_table_schema(param)
            .await?;

        Ok(build_evolve_table_schema_response(table))
    }
}
