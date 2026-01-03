use crate::application::data_manipulation::get_current_state_param::GetCurrentStateParam;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::grpc::proto::GetCurrentStateRequest;
use crate::util::error::ParameterError;
use tonic::Request;

pub(super) fn build_get_current_state_param(
    request: Request<GetCurrentStateRequest>,
) -> Result<GetCurrentStateParam, ParameterError> {
    let req = request.get_ref();

    let param = GetCurrentStateParam {
        stream: UserTablStream::new(req.table_id.into(), req.stream_id.into()),
    };
    Ok(param)
}
