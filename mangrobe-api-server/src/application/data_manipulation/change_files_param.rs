use crate::application::data_manipulation::param_util::{to_file_lock_key, to_partition_time};
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawChangeFilesEntry;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::grpc::proto::ChangeFilesRequest;
use crate::util::error::ParameterError;
use chrono::{DateTime, Utc};

pub struct ChangeFilesParam {
    pub file_lock_key: FileLockKey,
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,
    pub entry: ChangeRequestRawChangeFilesEntry,
}

impl ChangeFilesParam {
    pub fn new(
        request: &ChangeFilesRequest,
        request_started_at: DateTime<Utc>,
    ) -> Result<Self, ParameterError> {
        let partition_time = to_partition_time(request.partition_time)?;
        let file_lock_key = to_file_lock_key(request.file_lock_key.clone(), request_started_at)?;

        let files_to_add = request
            .file_delete_entries
            .iter()
            .map(|f| f.path.clone().into())
            .collect();

        let param = Self {
            file_lock_key,
            user_table_id: request.table_id.into(),
            stream_id: request.stream_id.into(),
            partition_time,
            entry: ChangeRequestRawChangeFilesEntry::new(files_to_add),
        };
        Ok(param)
    }
}
