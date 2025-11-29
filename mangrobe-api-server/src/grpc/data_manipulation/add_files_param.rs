use crate::application::data_manipulation::add_files_param::AddFilesParam;
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawAddFilesEntry;
use crate::domain::model::file::FileEntry;
use crate::grpc::proto::AddFilesRequest;
use crate::grpc::util::param_util::{to_idempotency_key, to_partition_time};
use crate::util::error::ParameterError;
use tonic::Request;

pub(super) fn build_add_files_param(
    request: Request<AddFilesRequest>,
) -> Result<AddFilesParam, ParameterError> {
    let req = request.get_ref();

    let partition_time = to_partition_time(req.partition_time)?;
    let idempotency_key = to_idempotency_key(req.idempotency_key.clone())?;

    let files = req
        .file_add_entries
        .iter()
        .map(|f| FileEntry::new(f.path.clone().into(), f.size))
        .collect();

    let param = AddFilesParam {
        idempotency_key,
        user_table_id: req.table_id.into(),
        stream_id: req.stream_id.into(),
        partition_time,
        entry: ChangeRequestRawAddFilesEntry::new(files),
    };
    Ok(param)
}
