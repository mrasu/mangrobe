use crate::api::core::information_schema::information_schema_service::InformationSchemaService as CoreInformationSchemaService;
use crate::api::grpc::proto::{
    ListStreamsRequest, ListStreamsResponse, information_schema_service_server,
};
use crate::api::grpc::util::error::to_grpc_error;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub(crate) struct InformationSchemaService {
    core_service: CoreInformationSchemaService,
}

impl InformationSchemaService {
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            core_service: CoreInformationSchemaService::new(db),
        }
    }
}

#[tonic::async_trait]
impl information_schema_service_server::InformationSchemaService for InformationSchemaService {
    async fn list_streams(
        &self,
        request: Request<ListStreamsRequest>,
    ) -> Result<Response<ListStreamsResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .core_service
            .list_streams(req)
            .await
            .map_err(to_grpc_error)?;

        Ok(Response::new(res))
    }
}
