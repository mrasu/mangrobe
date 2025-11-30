use crate::application::data_manipulation::add_files_param::AddFilesParam;
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawAddFileEntry;
use crate::domain::model::file::FileEntry;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::grpc::proto::AddFilesRequest;
use crate::grpc::util::param_util::{to_idempotency_key, to_partition_time};
use crate::util::error::ParameterError;
use tonic::Request;

pub(super) fn build_add_files_param(
    request: Request<AddFilesRequest>,
) -> Result<AddFilesParam, ParameterError> {
    let req = request.get_ref();

    let idempotency_key = to_idempotency_key(req.idempotency_key.clone())?;

    let mut entries = vec![];
    for entry in req.add_file_entries.iter() {
        let partition_time = to_partition_time(entry.partition_time)?;
        entries.push(ChangeRequestRawAddFileEntry::new(
            partition_time,
            entry
                .file_info_entries
                .iter()
                .map(|f| FileEntry::new(f.path.clone().into(), f.size))
                .collect(),
        ))
    }

    let param = AddFilesParam {
        idempotency_key,
        stream: UserTablStream::new(req.table_id.into(), req.stream_id.into()),
        entries,
    };
    Ok(param)
}
