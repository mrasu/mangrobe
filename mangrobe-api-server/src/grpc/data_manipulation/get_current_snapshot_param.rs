use crate::application::data_manipulation::get_current_snapshot_param::GetCurrentSnapshotParam;
use crate::grpc::proto::GetCurrentSnapshotRequest;
use crate::util::error::ParameterError;
use tonic::Request;

pub(super) fn build_get_current_snapshot_param(
    request: Request<GetCurrentSnapshotRequest>,
) -> Result<GetCurrentSnapshotParam, ParameterError> {
    let req = request.get_ref();

    let param = GetCurrentSnapshotParam {
        user_table_id: req.table_id.into(),
        stream_id: req.stream_id.into(),
    };
    Ok(param)
}
