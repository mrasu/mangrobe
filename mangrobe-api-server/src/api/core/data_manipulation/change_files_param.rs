use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::file_lock_key::to_file_lock_key;
use crate::api::core::util::param::partition_time::to_partition_time;
use crate::api::core::util::param::table_name::to_table_name;
use crate::api::grpc::proto::ChangeFilesRequest;
use crate::application::data_manipulation::change_files_param::ChangeFilesParam;
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawChangeFilesEntry;
use chrono::{DateTime, Utc};

pub(crate) fn build_change_file_param(
    req: &ChangeFilesRequest,
    request_started_at: DateTime<Utc>,
) -> Result<ChangeFilesParam, ParameterError> {
    let table_name = to_table_name(req.table_name.clone())?;

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
        table_name,
        stream_id: req.stream_id.into(),
        entries,
    };
    Ok(param)
}
