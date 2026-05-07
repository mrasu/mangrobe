use crate::api::core::information_schema::list_streams_response::build_list_streams_response;
use crate::api::grpc::information_schema::list_streams_param::parse_list_streams_param;
use crate::api::grpc::proto::{
    ListStreamsRequest, ListStreamsResponse, information_schema_service_server,
};
use crate::api::grpc::util::error::{build_invalid_argument, to_grpc_error};
use crate::application::information_schema::information_schema_use_case::InformationSchemaUseCase;
use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

pub(crate) struct InformationSchemaService {
    information_schema_use_case: InformationSchemaUseCase,
}

impl InformationSchemaService {
    pub fn new(db: &DatabaseConnection) -> Self {
        Self {
            information_schema_use_case: InformationSchemaUseCase::new(db.clone()),
        }
    }
}

#[tonic::async_trait]
impl information_schema_service_server::InformationSchemaService for InformationSchemaService {
    async fn list_streams(
        &self,
        request: Request<ListStreamsRequest>,
    ) -> Result<Response<ListStreamsResponse>, Status> {
        let (param, page_size) =
            parse_list_streams_param(request).map_err(build_invalid_argument)?;

        let streams = self
            .information_schema_use_case
            .list_streams(&param, (page_size + 1) as u64)
            .await
            .map_err(to_grpc_error)?;

        let response = build_list_streams_response(&param.table_name, page_size as usize, &streams);
        Ok(Response::new(response))
    }
}
