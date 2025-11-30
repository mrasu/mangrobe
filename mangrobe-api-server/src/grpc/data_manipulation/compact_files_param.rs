use crate::application::data_manipulation::compact_files_param::CompactFilesParam;
use crate::domain::model::change_request_raw_file_entry::{
    ChangeRequestRawCompactFileInfoEntry,
    ChangeRequestRawCompactFilesEntry,
};
use crate::domain::model::file::FileEntry;
use crate::grpc::proto::CompactFilesRequest;
use crate::grpc::util::param_util::{to_file_lock_key, to_partition_time};
use crate::util::error::ParameterError;
use chrono::{DateTime, Utc};
use tonic::Request;

pub(super) fn build_compact_files_param(
    request: Request<CompactFilesRequest>,
    request_started_at: DateTime<Utc>,
) -> Result<CompactFilesParam, ParameterError> {
    let req = request.get_ref();

    let file_lock_key = to_file_lock_key(req.file_lock_key.clone(), request_started_at)?;

    let mut entries = vec![];
    for entry in req.compact_file_entries.iter() {
        let partition_time = to_partition_time(entry.partition_time)?;

        let mut file_info_entries = vec![];
        for info_entry in entry.file_info_entries.iter() {
            let src_file_paths: Vec<_> = info_entry
                .src_entries
                .iter()
                .map(|f| f.path.clone().into())
                .collect();
            if src_file_paths.is_empty() {
                return Err(ParameterError::Required("src_file_entries".to_string()));
            }

            let Some(ref req_dst_file) = info_entry.dst_entry else {
                return Err(ParameterError::Required("dst_file_entry".to_string()));
            };
            let dst_file = FileEntry::new(req_dst_file.path.clone().into(), req_dst_file.size);
            file_info_entries.push(ChangeRequestRawCompactFileInfoEntry::new(
                src_file_paths,
                dst_file,
            ))
        }
        entries.push(ChangeRequestRawCompactFilesEntry::new(
            partition_time,
            file_info_entries,
        ))
    }

    let param = CompactFilesParam {
        file_lock_key,
        user_table_id: req.table_id.into(),
        stream_id: req.stream_id.into(),
        entries,
    };
    Ok(param)
}
