use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::table_name::to_table_name;
use crate::api::grpc::proto::CreateTableRequest;
use crate::application::data_definition::create_table_param::CreateTableParam;

pub(crate) fn build_create_table_param(
    req: &CreateTableRequest,
) -> Result<CreateTableParam, ParameterError> {
    let table_name = to_table_name(req.table_name.clone())?;

    Ok(CreateTableParam {
        table_name,
        skip_if_exists: req.skip_if_exists,
    })
}
