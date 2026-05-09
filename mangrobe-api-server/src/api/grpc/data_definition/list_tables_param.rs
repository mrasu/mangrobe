use crate::api::core::data_definition::list_tables_param::parse_list_tables_param as parse_api_list_tables_param;
use crate::api::grpc::proto::ListTablesRequest;
use crate::application::data_definition::list_tables_param::ListTablesParam;
use tonic::Request;

pub(super) fn parse_list_tables_param(
    request: Request<ListTablesRequest>,
) -> ListTablesParam {
    let req = request.into_inner();
    parse_api_list_tables_param(&req)
}
