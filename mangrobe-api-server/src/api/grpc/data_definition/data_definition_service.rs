use crate::api::core::data_definition::data_definition_service::DataDefinitionService as CoreDataDefinitionService;
use crate::api::grpc::proto::{
    CreateExternalTableRequest, CreateExternalTableResponse, CreateTableRequest,
    CreateTableResponse, EvolveTableSchemaRequest, EvolveTableSchemaResponse, GetTableRequest,
    GetTableResponse, ListTablesRequest, ListTablesResponse, data_definition_service_server,
};
use crate::api::grpc::util::error::to_grpc_error;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub(crate) struct DataDefinitionService {
    core_service: CoreDataDefinitionService,
}

impl DataDefinitionService {
    pub fn new(db: &DatabaseConnection) -> Self {
        Self {
            core_service: CoreDataDefinitionService::new(db.clone()),
        }
    }
}

#[tonic::async_trait]
impl data_definition_service_server::DataDefinitionService for DataDefinitionService {
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .create_table(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn create_external_table(
        &self,
        request: Request<CreateExternalTableRequest>,
    ) -> Result<Response<CreateExternalTableResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .create_external_table(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .get_table(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn list_tables(
        &self,
        request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .list_tables(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }

    async fn evolve_table_schema(
        &self,
        request: Request<EvolveTableSchemaRequest>,
    ) -> Result<Response<EvolveTableSchemaResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .evolve_table_schema(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }
}
