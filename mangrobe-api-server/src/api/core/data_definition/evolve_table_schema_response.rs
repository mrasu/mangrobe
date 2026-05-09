use crate::api::core::util::param::table_definition::to_proto_table_definition;
use crate::api::grpc::proto::EvolveTableSchemaResponse;
use crate::domain::model::table_definition::TableDefinition;

pub(crate) fn build_evolve_table_schema_response(
    table: TableDefinition,
) -> EvolveTableSchemaResponse {
    EvolveTableSchemaResponse {
        table: Some(to_proto_table_definition(table)),
    }
}
