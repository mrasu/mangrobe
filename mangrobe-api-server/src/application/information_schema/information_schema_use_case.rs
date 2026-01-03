use crate::application::information_schema::list_streams_param::ListStreamsParam;
use crate::domain::model::stream::Stream;
use crate::domain::service::stream_service::StreamService;
use sea_orm::DatabaseConnection;

pub struct InformationSchemaUseCase {
    stream_service: StreamService,
}

impl InformationSchemaUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            stream_service: StreamService::new(&connection),
        }
    }

    pub async fn list_streams(
        &self,
        param: &ListStreamsParam,
        limit: u64,
    ) -> Result<Vec<Stream>, anyhow::Error> {
        self.stream_service
            .find_streams_after(&param.table_id, &param.stream_id_after, limit)
            .await
    }
}
