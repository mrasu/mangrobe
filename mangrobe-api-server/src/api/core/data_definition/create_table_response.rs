use crate::api::core::util::param::table_definition::to_proto_table_definition;
use crate::api::grpc::proto::CreateTableResponse;
use crate::domain::model::table_definition::TableDefinition;

pub(crate) fn build_create_table_response(table: TableDefinition) -> CreateTableResponse {
    CreateTableResponse {
        table: Some(to_proto_table_definition(table)),
    }
}
