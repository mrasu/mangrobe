use crate::application::data_definition::CreateTableParam;
use crate::grpc::proto::CreateTableRequest;
use crate::grpc::util::param_util::to_table_name;
use crate::util::error::ParameterError;
use tonic::Request;

pub(super) fn build_create_table_param(
    request: Request<CreateTableRequest>,
) -> Result<CreateTableParam, ParameterError> {
    let req = request.get_ref();
    let table_name = to_table_name(req.table_name.clone())?;

    Ok(CreateTableParam {
        table_name,
        skip_if_exists: req.skip_if_exists,
    })
}
