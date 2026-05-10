use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::file_lock_key::to_file_lock_key;
use crate::api::core::util::param::partition_time::to_partition_time;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::AcquireFileLockRequest;
use crate::application::lock_control::acquire_file_lock_param::AcquireFileLockParam;
use crate::domain::model::lock_raw_file_entry::LockFileRawAcquireEntry;
use chrono::{DateTime, Duration, Utc};

pub(crate) fn build_acquire_file_lock_param(
    req: &AcquireFileLockRequest,
    request_started_at: DateTime<Utc>,
) -> Result<AcquireFileLockParam, ParameterError> {
    let table_identifier =
        to_table_identifier(required("table_identifier", req.table_identifier.as_ref())?)?;

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
        table_identifier,
        stream_id: req.stream_id.into(),
        ttl: Duration::seconds(req.ttl_sec),
        entries,
    };
    Ok(param)
}
