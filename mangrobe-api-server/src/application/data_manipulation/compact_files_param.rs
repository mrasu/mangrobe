use crate::application::data_manipulation::param_util::{to_idempotency_key, to_partition_time};
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawCompactFilesEntry;
use crate::domain::model::file::{FileEntry, FilePath};
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::grpc::proto::CompactFilesRequest;
use crate::util::error::ParameterError;
use chrono::{DateTime, Utc};

pub struct CompactFilesParam {
    pub idempotency_key: IdempotencyKey,
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,
    pub entry: ChangeRequestRawCompactFilesEntry,
}

impl TryFrom<&CompactFilesRequest> for CompactFilesParam {
    type Error = ParameterError;

    fn try_from(request: &CompactFilesRequest) -> Result<Self, Self::Error> {
        let partition_time = to_partition_time(request.partition_time)?;
        let idempotency_key = to_idempotency_key(request.idempotency_key.clone())?;

        let src_file_paths: Vec<_> = request
            .src_file_entries
            .iter()
            .map(|f| f.path.clone().into())
            .collect();
        if src_file_paths.is_empty() {
            return Err(ParameterError::Required("src_file_entries".to_string()));
        }

        let Some(ref req_dst_file) = request.dst_file_entry else {
            return Err(ParameterError::Required("dst_file_entry".to_string()));
        };
        let dst_file = FileEntry::new(req_dst_file.path.clone().into(), req_dst_file.size);

        let param = Self {
            idempotency_key,
            user_table_id: request.table_id.into(),
            stream_id: request.stream_id.into(),
            partition_time,
            entry: ChangeRequestRawCompactFilesEntry::new(src_file_paths, dst_file),
        };
        Ok(param)
    }
}
