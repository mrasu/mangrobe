use crate::api::core::data_definition::create_table_param::build_create_table_param as build_api_create_table_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::CreateTableRequest;
use crate::application::data_definition::create_table_param::CreateTableParam;
use tonic::Request;

pub(super) fn build_create_table_param(
    request: Request<CreateTableRequest>,
) -> Result<CreateTableParam, ParameterError> {
    let req = request.get_ref();
    build_api_create_table_param(req)
}
