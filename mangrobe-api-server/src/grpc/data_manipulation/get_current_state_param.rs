use crate::application::data_manipulation::get_current_state_param::GetCurrentStateParam;
use crate::grpc::proto::GetCurrentStateRequest;
use crate::grpc::util::param_util::to_table_name;
use crate::util::error::ParameterError;
use tonic::Request;

pub(super) fn build_get_current_state_param(
    request: Request<GetCurrentStateRequest>,
) -> Result<GetCurrentStateParam, ParameterError> {
    let req = request.get_ref();
    let table_name = to_table_name(req.table_name.clone())?;

    let param = GetCurrentStateParam {
        table_name,
        stream_id: req.stream_id.into(),
    };
    Ok(param)
}
