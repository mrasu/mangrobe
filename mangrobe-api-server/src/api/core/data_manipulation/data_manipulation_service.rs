use crate::api::core::data_manipulation::add_files_param::build_add_files_param;
use crate::api::core::data_manipulation::add_files_response::build_add_files_response;
use crate::api::core::data_manipulation::change_files_param::build_change_file_param;
use crate::api::core::data_manipulation::change_files_response::build_change_files_response;
use crate::api::core::data_manipulation::compact_files_param::build_compact_files_param;
use crate::api::core::data_manipulation::compact_files_response::build_compact_files_response;
use crate::api::core::data_manipulation::get_commits_param::build_get_commits_param;
use crate::api::core::data_manipulation::get_commits_response::build_get_commits_response;
use crate::api::core::data_manipulation::get_current_state_param::build_get_current_state_param;
use crate::api::core::data_manipulation::get_current_state_response::build_get_current_state_response;
use crate::api::core::data_manipulation::get_file_info_param::build_get_file_info_param;
use crate::api::core::data_manipulation::get_file_info_response::build_get_file_info_response;
use crate::api::grpc::proto::{
    AddFilesRequest, AddFilesResponse, ChangeFilesRequest, ChangeFilesResponse,
    CompactFilesRequest, CompactFilesResponse, GetCommitsRequest, GetCommitsResponse,
    GetCurrentStateRequest, GetCurrentStateResponse, GetFileInfoRequest, GetFileInfoResponse,
};
use crate::application::data_manipulation::data_manipulation_use_case::DataManipulationUseCase;
use chrono::Utc;
use sea_orm::DatabaseConnection;

const CHANGES_LIMIT_PER_STREAM: u64 = 100;

pub struct DataManipulationService {
    data_manipulation_use_case: DataManipulationUseCase,
}

impl DataManipulationService {
    pub(crate) fn new(db: DatabaseConnection) -> Self {
        Self {
            data_manipulation_use_case: DataManipulationUseCase::new(db),
        }
    }

    pub async fn get_current_state(
        &self,
        param: GetCurrentStateRequest,
    ) -> Result<GetCurrentStateResponse, anyhow::Error> {
        let param = build_get_current_state_param(&param)?;

        let snapshot = self
            .data_manipulation_use_case
            .get_current_state(param)
            .await?;

        Ok(build_get_current_state_response(snapshot))
    }

    pub async fn get_commits(
        &self,
        param: GetCommitsRequest,
    ) -> Result<GetCommitsResponse, anyhow::Error> {
        let param = build_get_commits_param(&param)?;

        let changes = self
            .data_manipulation_use_case
            .get_commits(&param, CHANGES_LIMIT_PER_STREAM)
            .await?;

        Ok(build_get_commits_response(param.table_identifier, changes))
    }

    pub async fn get_file_info(
        &self,
        param: GetFileInfoRequest,
    ) -> Result<GetFileInfoResponse, anyhow::Error> {
        let application_param = build_get_file_info_param(&param)?;
        let file_with_stats = self
            .data_manipulation_use_case
            .get_file_with_stat(application_param.clone())
            .await?;

        Ok(build_get_file_info_response(
            &application_param,
            file_with_stats,
        ))
    }

    pub async fn add_files(
        &self,
        param: AddFilesRequest,
    ) -> Result<AddFilesResponse, anyhow::Error> {
        let param = build_add_files_param(&param)?;
        let commit_id = self.data_manipulation_use_case.add_files(param).await?;

        Ok(build_add_files_response(commit_id))
    }

    pub async fn change_files(
        &self,
        param: ChangeFilesRequest,
    ) -> Result<ChangeFilesResponse, anyhow::Error> {
        let request_started_at = Utc::now();
        let param = build_change_file_param(&param, request_started_at)?;
        let commit_id = self.data_manipulation_use_case.change_files(param).await?;

        Ok(build_change_files_response(commit_id))
    }

    pub async fn compact_files(
        &self,
        param: CompactFilesRequest,
    ) -> Result<CompactFilesResponse, anyhow::Error> {
        let request_started_at = Utc::now();
        let param = build_compact_files_param(&param, request_started_at)?;
        let commit_id = self.data_manipulation_use_case.compact_files(param).await?;

        Ok(build_compact_files_response(commit_id))
    }
}
