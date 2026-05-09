use crate::api::core::data_definition::create_external_table_param::build_create_external_table_param as build_api_create_external_table_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::CreateExternalTableRequest;
use crate::application::data_definition::create_external_table_param::CreateExternalTableParam;
use tonic::Request;

pub(super) fn build_create_external_table_param(
    request: Request<CreateExternalTableRequest>,
) -> Result<CreateExternalTableParam, ParameterError> {
    let req = request.get_ref();
    build_api_create_external_table_param(req)
}
