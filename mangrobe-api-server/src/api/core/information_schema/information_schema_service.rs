use crate::api::core::information_schema::list_streams_param::parse_list_streams_param;
use crate::api::core::information_schema::list_streams_response::build_list_streams_response;
use crate::api::grpc::proto::{ListStreamsRequest, ListStreamsResponse};
use crate::application::information_schema::information_schema_use_case::InformationSchemaUseCase;
use sea_orm::DatabaseConnection;

pub struct InformationSchemaService {
    information_schema_use_case: InformationSchemaUseCase,
}

impl InformationSchemaService {
    pub(crate) fn new(db: DatabaseConnection) -> Self {
        Self {
            information_schema_use_case: InformationSchemaUseCase::new(db),
        }
    }

    pub async fn list_streams(
        &self,
        param: ListStreamsRequest,
    ) -> Result<ListStreamsResponse, anyhow::Error> {
        let (param, page_size) = parse_list_streams_param(&param)?;

        let streams = self
            .information_schema_use_case
            .list_streams(&param, (page_size + 1) as u64)
            .await?;

        Ok(build_list_streams_response(
            &param.table_name,
            page_size as usize,
            &streams,
        ))
    }
}
