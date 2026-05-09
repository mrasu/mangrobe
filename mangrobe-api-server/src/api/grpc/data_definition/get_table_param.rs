use crate::api::core::data_definition::get_table_param::build_get_table_param as build_api_get_table_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::GetTableRequest;
use crate::application::data_definition::get_table_param::GetTableParam;
use tonic::Request;

pub(super) fn build_get_table_param(
    request: Request<GetTableRequest>,
) -> Result<GetTableParam, ParameterError> {
    let req = request.get_ref();
    build_api_get_table_param(req)
}
