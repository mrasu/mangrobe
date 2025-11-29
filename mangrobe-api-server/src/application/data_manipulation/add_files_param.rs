use crate::application::data_manipulation::param_util::{to_idempotency_key, to_partition_time};
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawAddFilesEntry;
use crate::domain::model::file::FileEntry;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::grpc::proto::AddFilesRequest;
use crate::util::error::ParameterError;
use chrono::{DateTime, Utc};

pub struct AddFilesParam {
    pub idempotency_key: IdempotencyKey,
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,
    pub entry: ChangeRequestRawAddFilesEntry,
}

impl TryFrom<&AddFilesRequest> for AddFilesParam {
    type Error = ParameterError;

    fn try_from(request: &AddFilesRequest) -> Result<Self, Self::Error> {
        let partition_time = to_partition_time(request.partition_time)?;
        let idempotency_key = to_idempotency_key(request.idempotency_key.clone())?;

        let files = request
            .file_add_entries
            .iter()
            .map(|f| FileEntry::new(f.path.clone().into(), f.size))
            .collect();

        let param = Self {
            idempotency_key,
            user_table_id: request.table_id.into(),
            stream_id: request.stream_id.into(),
            partition_time,
            entry: ChangeRequestRawAddFilesEntry::new(files),
        };
        Ok(param)
    }
}
