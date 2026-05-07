use crate::api::core::data_manipulation::get_file_info_param::build_get_file_info_param as build_api_get_file_info_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::GetFileInfoRequest;
use crate::application::data_manipulation::get_file_info_param::GetFileInfoParam;
use tonic::Request;

pub(super) fn build_get_file_info_param(
    request: Request<GetFileInfoRequest>,
) -> Result<GetFileInfoParam, ParameterError> {
    let req = request.get_ref();
    build_api_get_file_info_param(req)
}
