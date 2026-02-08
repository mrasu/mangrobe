use crate::application::information_schema::list_streams_param::ListStreamsParam;
use crate::application::util::user_table::find_table_id;
use crate::domain::model::stream::Stream;
use crate::domain::service::stream_service::StreamService;
use crate::domain::service::user_table_service::UserTableService;
use sea_orm::DatabaseConnection;

pub struct InformationSchemaUseCase {
    stream_service: StreamService,
    user_table_service: UserTableService,
}

impl InformationSchemaUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            stream_service: StreamService::new(&connection),
            user_table_service: UserTableService::new(&connection),
        }
    }

    pub async fn list_streams(
        &self,
        param: &ListStreamsParam,
        limit: u64,
    ) -> Result<Vec<Stream>, anyhow::Error> {
        let table_id = find_table_id(&self.user_table_service, &param.table_name).await?;
        self.stream_service
            .find_streams_after(&table_id, &param.stream_id_after, limit)
            .await
    }
}
