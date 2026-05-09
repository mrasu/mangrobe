use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::GetTableRequest;
use crate::application::data_definition::get_table_param::GetTableParam;

pub(crate) fn build_get_table_param(
    req: &GetTableRequest,
) -> Result<GetTableParam, ParameterError> {
    Ok(GetTableParam {
        identifier: to_table_identifier(required("identifier", req.identifier.as_ref())?)?,
    })
}
