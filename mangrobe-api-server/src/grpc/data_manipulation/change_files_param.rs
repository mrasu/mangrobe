use crate::application::data_manipulation::change_files_param::ChangeFilesParam;
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawChangeFilesEntry;
use crate::grpc::proto::ChangeFilesRequest;
use crate::grpc::util::param_util::{to_file_lock_key, to_partition_time};
use crate::util::error::ParameterError;
use chrono::{DateTime, Utc};
use tonic::Request;

pub(super) fn build_change_file_param(
    request: Request<ChangeFilesRequest>,
    request_started_at: DateTime<Utc>,
) -> Result<ChangeFilesParam, ParameterError> {
    let req = request.get_ref();

    let file_lock_key = to_file_lock_key(req.file_lock_key.clone(), request_started_at)?;

    let mut entries = vec![];
    for entry in req.change_file_entries.iter() {
        let partition_time = to_partition_time(entry.partition_time)?;
        entries.push(ChangeRequestRawChangeFilesEntry::new(
            partition_time,
            entry
                .delete_entries
                .iter()
                .map(|f| f.path.clone().into())
                .collect(),
        ))
    }

    let param = ChangeFilesParam {
        file_lock_key,
        user_table_id: req.table_id.into(),
        stream_id: req.stream_id.into(),
        entries,
    };
    Ok(param)
}
