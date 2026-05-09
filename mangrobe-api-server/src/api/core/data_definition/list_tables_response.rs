use crate::api::core::util::param::table_identifier::to_proto_table_identifier;
use crate::api::grpc::proto::{ListTablesResponse, TableSummary as ProtoTableSummary};
use crate::domain::model::table_summary::TableSummary;

pub(crate) fn build_list_tables_response(tables: Vec<TableSummary>) -> ListTablesResponse {
    ListTablesResponse {
        tables: tables.into_iter().map(to_proto_table_summary).collect(),
    }
}

fn to_proto_table_summary(table: TableSummary) -> ProtoTableSummary {
    ProtoTableSummary {
        identifier: Some(to_proto_table_identifier(table.identifier)),
        comment: table.comment,
    }
}
