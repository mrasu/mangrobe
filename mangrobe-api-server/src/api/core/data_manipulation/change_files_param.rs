use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::file_lock_key::to_file_lock_key;
use crate::api::core::util::param::partition::to_unvalidated_partition;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::ChangeFilesRequest;
use crate::application::data_manipulation::change_files_param::ChangeFilesParam;
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawChangeFilesEntry;
use chrono::{DateTime, Utc};

pub(crate) fn build_change_file_param(
    req: &ChangeFilesRequest,
    request_started_at: DateTime<Utc>,
) -> Result<ChangeFilesParam, ParameterError> {
    let table_identifier =
        to_table_identifier(required("table_identifier", req.table_identifier.as_ref())?)?;

    let file_lock_key = to_file_lock_key(req.file_lock_key.clone(), request_started_at)?;

    let mut entries = vec![];
    for entry in req.change_file_entries.iter() {
        let partition = to_unvalidated_partition(entry.partition.as_ref())?;
        entries.push(ChangeRequestRawChangeFilesEntry::new(
            partition,
            entry
                .delete_entries
                .iter()
                .map(|f| f.path.clone().into())
                .collect(),
        ))
    }

    let param = ChangeFilesParam {
        file_lock_key,
        table_identifier,
        stream: req.stream.into(),
        entries,
    };
    Ok(param)
}
