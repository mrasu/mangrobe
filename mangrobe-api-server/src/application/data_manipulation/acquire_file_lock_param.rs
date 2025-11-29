use crate::application::data_manipulation::param_util::{to_file_lock_key, to_partition_time};
use crate::domain::model::file::FilePath;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;
use crate::grpc::proto::AcquireFileLockRequest;
use crate::util::error::ParameterError;
use chrono::{DateTime, Duration, Utc};

pub struct AcquireFileLockParam {
    pub file_lock_key: FileLockKey,
    pub user_table_id: UserTableId,
    pub stream_id: StreamId,
    pub partition_time: DateTime<Utc>,
    pub ttl: Duration,
    pub paths: Vec<FilePath>,
}

impl AcquireFileLockParam {
    pub fn new(
        request: &AcquireFileLockRequest,
        request_started_at: DateTime<Utc>,
    ) -> Result<Self, ParameterError> {
        let partition_time = to_partition_time(request.partition_time)?;
        let file_lock_key = to_file_lock_key(request.file_lock_key.clone(), request_started_at)?;

        let paths: Vec<_> = request
            .target_files
            .iter()
            .map(|f| f.path.clone().into())
            .collect();
        if paths.is_empty() {
            return Err(ParameterError::Required("target_files".to_string()));
        }

        let param = Self {
            file_lock_key,
            user_table_id: request.table_id.into(),
            stream_id: request.stream_id.into(),
            partition_time,
            ttl: Duration::seconds(request.ttl_sec),
            paths,
        };
        Ok(param)
    }
}
