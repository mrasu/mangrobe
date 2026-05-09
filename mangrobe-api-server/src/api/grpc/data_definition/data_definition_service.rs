use crate::api::core::data_definition::create_external_table_response::build_create_external_table_response;
use crate::api::core::data_definition::create_table_response::build_create_table_response;
use crate::api::core::data_definition::get_table_response::build_get_table_response;
use crate::api::core::data_definition::list_tables_response::build_list_tables_response;
use crate::api::grpc::data_definition::create_external_table_param::build_create_external_table_param;
use crate::api::grpc::data_definition::create_table_param::build_create_table_param;
use crate::api::grpc::data_definition::get_table_param::build_get_table_param;
use crate::api::grpc::data_definition::list_tables_param::parse_list_tables_param;
use crate::api::grpc::proto::{
    CreateExternalTableRequest, CreateExternalTableResponse, CreateTableRequest,
    CreateTableResponse, GetTableRequest, GetTableResponse, ListTablesRequest, ListTablesResponse,
    data_definition_service_server,
};
use crate::api::grpc::util::error::{build_invalid_argument, to_grpc_error};
use crate::application::data_definition::data_definition_use_case::DataDefinitionUseCase;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub(crate) struct DataDefinitionService {
    data_definition_use_case: DataDefinitionUseCase,
}

impl DataDefinitionService {
    pub fn new(db: &DatabaseConnection) -> Self {
        Self {
            data_definition_use_case: DataDefinitionUseCase::new(db.clone()),
        }
    }
}

#[tonic::async_trait]
impl data_definition_service_server::DataDefinitionService for DataDefinitionService {
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let param = build_create_table_param(request).map_err(build_invalid_argument)?;

        let table = self
            .data_definition_use_case
            .create_table(param)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(build_create_table_response(table)))
    }

    async fn create_external_table(
        &self,
        request: Request<CreateExternalTableRequest>,
    ) -> Result<Response<CreateExternalTableResponse>, Status> {
        let param = build_create_external_table_param(request).map_err(build_invalid_argument)?;

        let table = self
            .data_definition_use_case
            .create_external_table(param)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(build_create_external_table_response(table)))
    }

    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        let param = build_get_table_param(request).map_err(build_invalid_argument)?;

        let table = self
            .data_definition_use_case
            .get_table(param)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(build_get_table_response(table)))
    }

    async fn list_tables(
        &self,
        request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        let param = parse_list_tables_param(request);

        let tables = self
            .data_definition_use_case
            .list_tables(param)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(build_list_tables_response(tables)))
    }
}
