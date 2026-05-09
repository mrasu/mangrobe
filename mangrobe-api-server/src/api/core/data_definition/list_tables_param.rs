use crate::api::grpc::proto::ListTablesRequest;
use crate::application::data_definition::list_tables_param::ListTablesParam;

pub(crate) fn parse_list_tables_param(req: &ListTablesRequest) -> ListTablesParam {
    ListTablesParam {
        catalog_name: req.catalog_name.clone(),
        schema_name: req.schema_name.clone(),
    }
}
