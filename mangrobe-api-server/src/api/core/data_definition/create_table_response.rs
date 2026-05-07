use crate::api::grpc::proto::CreateTableResponse;
use crate::domain::model::user_table::UserTable;

pub(crate) fn build_create_table_response(table: UserTable) -> CreateTableResponse {
    CreateTableResponse {
        table_name: table.name.val(),
    }
}
