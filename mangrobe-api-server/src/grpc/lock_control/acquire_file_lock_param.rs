use crate::application::lock_control::acquire_file_lock_param::AcquireFileLockParam;
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

    let partition_time = to_partition_time(req.partition_time)?;
    let file_lock_key = to_file_lock_key(req.file_lock_key.clone(), request_started_at)?;

    let paths: Vec<_> = req
        .target_files
        .iter()
        .map(|f| f.path.clone().into())
        .collect();
    if paths.is_empty() {
        return Err(ParameterError::Required("target_files".to_string()));
    }

    let param = AcquireFileLockParam {
        file_lock_key,
        user_table_id: req.table_id.into(),
        stream_id: req.stream_id.into(),
        partition_time,
        ttl: Duration::seconds(req.ttl_sec),
        paths,
    };
    Ok(param)
}
