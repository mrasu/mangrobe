use crate::api::core::data_manipulation::compact_files_param::build_compact_files_param as build_api_compact_files_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::CompactFilesRequest;
use crate::application::data_manipulation::compact_files_param::CompactFilesParam;
use chrono::{DateTime, Utc};
use tonic::Request;

pub(super) fn build_compact_files_param(
    request: Request<CompactFilesRequest>,
    request_started_at: DateTime<Utc>,
) -> Result<CompactFilesParam, ParameterError> {
    let req = request.get_ref();
    build_api_compact_files_param(req, request_started_at)
}
