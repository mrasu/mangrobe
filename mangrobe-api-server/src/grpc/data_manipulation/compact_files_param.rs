use crate::application::data_manipulation::compact_files_param::CompactFilesParam;
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawCompactFilesEntry;
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

    let partition_time = to_partition_time(req.partition_time)?;
    let file_lock_key = to_file_lock_key(req.file_lock_key.clone(), request_started_at)?;

    let src_file_paths: Vec<_> = req
        .src_file_entries
        .iter()
        .map(|f| f.path.clone().into())
        .collect();
    if src_file_paths.is_empty() {
        return Err(ParameterError::Required("src_file_entries".to_string()));
    }

    let Some(ref req_dst_file) = req.dst_file_entry else {
        return Err(ParameterError::Required("dst_file_entry".to_string()));
    };
    let dst_file = FileEntry::new(req_dst_file.path.clone().into(), req_dst_file.size);

    let param = CompactFilesParam {
        file_lock_key,
        user_table_id: req.table_id.into(),
        stream_id: req.stream_id.into(),
        partition_time,
        entry: ChangeRequestRawCompactFilesEntry::new(src_file_paths, dst_file),
    };
    Ok(param)
}
