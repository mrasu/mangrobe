use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::grpc::proto::GetCurrentSnapshotRequest;
use crate::util::error::ParameterError;

pub struct GetCurrentSnapshotParam {
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
}

impl TryFrom<&GetCurrentSnapshotRequest> for GetCurrentSnapshotParam {
    type Error = ParameterError;

    fn try_from(request: &GetCurrentSnapshotRequest) -> Result<Self, Self::Error> {
        let param = Self {
            user_table_id: request.table_id.into(),
            stream_id: request.stream_id.into(),
        };
        Ok(param)
    }
}
