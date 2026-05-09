use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::table_definition::to_column;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::EvolveTableSchemaRequest;
use crate::application::data_definition::evolve_table_schema_param::EvolveTableSchemaParam;

pub(crate) fn build_evolve_table_schema_param(
    req: &EvolveTableSchemaRequest,
) -> Result<EvolveTableSchemaParam, ParameterError> {
    Ok(EvolveTableSchemaParam {
        identifier: to_table_identifier(required("identifier", req.identifier.as_ref())?)?,
        proposed_columns: req
            .proposed_columns
            .iter()
            .map(to_column)
            .collect::<Result<Vec<_>, _>>()?,
    })
}
