use crate::application::lock_control::acquire_file_lock_param::AcquireFileLockParam;
use crate::domain::model::lock_raw_file_entry::LockFileRawAcquireEntry;
use crate::grpc::proto::AcquireFileLockRequest;
use crate::grpc::util::param_util::{to_file_lock_key, to_partition_time};
use crate::util::error::ParameterError;
use chrono::{DateTime, Duration, Utc};
use tonic::Request;

pub fn build_acquire_file_lock_param(
    request: Request<AcquireFileLockRequest>,
    request_started_at: DateTime<Utc>,
) -> Result<AcquireFileLockParam, ParameterError> {
    let req = request.get_ref();

    let file_lock_key = to_file_lock_key(req.file_lock_key.clone(), request_started_at)?;

    let mut entries = vec![];
    for entry in req.acquire_file_lock_entries.iter() {
        let partition_time = to_partition_time(entry.partition_time)?;
        entries.push(LockFileRawAcquireEntry::new(
            partition_time,
            entry
                .acquire_file_info_entries
                .iter()
                .map(|f| f.path.clone().into())
                .collect(),
        ))
    }

    let param = AcquireFileLockParam {
        file_lock_key,
        user_table_id: req.table_id.into(),
        stream_id: req.stream_id.into(),
        ttl: Duration::seconds(req.ttl_sec),
        entries,
    };
    Ok(param)
}
