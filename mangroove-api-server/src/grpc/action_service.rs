use crate::application::action_use_case::ActionUseCase;
use crate::domain::change_log::{AddedFile, ChangedFiles};
use crate::grpc::proto::{ChangeFilesRequest, ChangeFilesResponse, action_service_server};
use crate::grpc::util::to_internal_error;
use sea_orm::DatabaseConnection;
use sea_orm::sqlx::types::chrono::DateTime;
use tonic::{Code, Request, Response, Status};

pub struct ActionService {
    action_use_case: ActionUseCase,
}

impl ActionService {
    pub fn new(db: &DatabaseConnection) -> Self {
        let action_use_case = ActionUseCase::new(db.clone());
        Self { action_use_case }
    }
}

#[tonic::async_trait]
impl action_service_server::ActionService for ActionService {
    async fn change_files(
        &self,
        request: Request<ChangeFilesRequest>,
    ) -> Result<Response<ChangeFilesResponse>, Status> {
        let req = request.get_ref();
        let added_files = req
            .added_files
            .iter()
            .map(|f| AddedFile::new(f.path.clone(), f.size))
            .collect();
        let changed_files = &ChangedFiles::new(added_files);

        let req_partition_time = req
            .partition_time
            .unwrap_or(prost_types::Timestamp::default());
        let partition_time =
            DateTime::from_timestamp(req_partition_time.seconds, req_partition_time.nanos as u32);

        let Some(partition_time) = partition_time else {
            return Err(Status::new(
                Code::InvalidArgument,
                "partition_time is invalid. out-of-range number of seconds or nanos",
            ));
        };

        let change_log_id = self
            .action_use_case
            .change_files(
                req.idempotency_key.clone(),
                req.tenant_id,
                partition_time,
                changed_files,
            )
            .await
            .map_err(to_internal_error)?;

        let response = ChangeFilesResponse {
            change_log_id: change_log_id.into(),
        };
        Ok(Response::new(response))
    }
}
