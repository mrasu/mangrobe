use crate::application::data_manipulation::add_files_param::AddFilesParam;
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawAddFileEntry;
use crate::domain::model::file::FileEntry;
use crate::domain::model::file_column_statistics::FileColumnStatistics;
use crate::domain::model::file_metadata::FileMetadata;
use crate::grpc::proto::AddFilesRequest;
use crate::grpc::util::param_util::{to_idempotency_key, to_partition_time, to_table_name};
use crate::util::error::ParameterError;
use tonic::Request;

pub(super) fn build_add_files_param(
    request: Request<AddFilesRequest>,
) -> Result<AddFilesParam, ParameterError> {
    let req = request.get_ref();
    let table_name = to_table_name(req.table_name.clone())?;

    let idempotency_key = to_idempotency_key(req.idempotency_key.clone())?;

    let mut entries = vec![];
    for entry in req.add_file_entries.iter() {
        let partition_time = to_partition_time(entry.partition_time)?;
        entries.push(ChangeRequestRawAddFileEntry::new(
            partition_time,
            entry
                .file_info_entries
                .iter()
                .map(|f| {
                    let mut stats = Vec::with_capacity(f.column_statistics.len());
                    for statistics in &f.column_statistics {
                        if statistics.column_name.is_empty() {
                            return Err(ParameterError::Required("column_name".to_string()));
                        }
                        stats.push(FileColumnStatistics::new(
                            statistics.column_name.clone(),
                            statistics.min,
                            statistics.max,
                        ));
                    }

                    let file_metadata = f
                        .file_metadata
                        .as_ref()
                        .map(|metadata| FileMetadata::new(metadata.parquet_metadata.clone()));

                    Ok(FileEntry::new(
                        f.path.clone().into(),
                        f.size,
                        stats,
                        file_metadata,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    let param = AddFilesParam {
        idempotency_key,
        table_name,
        stream_id: req.stream_id.into(),
        entries,
    };
    Ok(param)
}
