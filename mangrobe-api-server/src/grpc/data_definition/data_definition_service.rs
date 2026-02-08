use crate::application::data_definition::data_definition_use_case::DataDefinitionUseCase;
use crate::grpc::data_definition::create_table_param::build_create_table_param;
use crate::grpc::proto::{CreateTableRequest, CreateTableResponse, data_definition_service_server};
use crate::grpc::util::error::{build_invalid_argument, to_grpc_error};
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub struct DataDefinitionService {
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

        Ok(Response::new(CreateTableResponse {
            table_name: table.name.val(),
        }))
    }
}
