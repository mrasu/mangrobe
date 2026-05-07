use crate::api::core::data_manipulation::get_commits_param::build_get_commits_param as build_api_get_commits_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::GetCommitsRequest;
use crate::application::data_manipulation::get_commits_param::GetCommitsParam;
use tonic::Request;

pub(super) fn build_get_commits_param(
    request: Request<GetCommitsRequest>,
) -> Result<GetCommitsParam, ParameterError> {
    let req = request.get_ref();
    build_api_get_commits_param(req)
}
