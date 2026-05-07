use crate::api::core::information_schema::list_streams_param::parse_list_streams_param as parse_api_list_streams_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::ListStreamsRequest;
use crate::application::information_schema::list_streams_param::ListStreamsParam;
use tonic::Request;

pub(super) fn parse_list_streams_param(
    request: Request<ListStreamsRequest>,
) -> Result<(ListStreamsParam, i32), ParameterError> {
    let req = request.get_ref();
    parse_api_list_streams_param(req)
}
