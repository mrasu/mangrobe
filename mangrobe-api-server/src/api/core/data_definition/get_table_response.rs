use crate::api::core::util::param::table_definition::to_proto_table_definition;
use crate::api::grpc::proto::GetTableResponse;
use crate::domain::model::table_definition::TableDefinition;

pub(crate) fn build_get_table_response(table: TableDefinition) -> GetTableResponse {
    GetTableResponse {
        table: Some(to_proto_table_definition(table)),
    }
}
