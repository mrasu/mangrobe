use crate::api::core::data_manipulation::change_files_param::build_change_file_param as build_api_change_file_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::ChangeFilesRequest;
use crate::application::data_manipulation::change_files_param::ChangeFilesParam;
use chrono::{DateTime, Utc};
use tonic::Request;

pub(super) fn build_change_file_param(
    request: Request<ChangeFilesRequest>,
    request_started_at: DateTime<Utc>,
) -> Result<ChangeFilesParam, ParameterError> {
    let req = request.get_ref();
    build_api_change_file_param(req, request_started_at)
}
