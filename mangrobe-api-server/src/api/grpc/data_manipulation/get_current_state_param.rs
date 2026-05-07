use crate::api::core::data_manipulation::get_current_state_param::build_get_current_state_param as build_api_get_current_state_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::GetCurrentStateRequest;
use crate::application::data_manipulation::get_current_state_param::GetCurrentStateParam;
use tonic::Request;

pub(super) fn build_get_current_state_param(
    request: Request<GetCurrentStateRequest>,
) -> Result<GetCurrentStateParam, ParameterError> {
    let req = request.get_ref();
    build_api_get_current_state_param(req)
}
