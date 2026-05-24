use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::idempotency_key::to_idempotency_key;
use crate::api::core::util::param::partition::to_unvalidated_partition;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::AddFilesRequest;
use crate::application::data_manipulation::add_files_param::AddFilesParam;
use crate::domain::model::change_request_raw_file_entry::ChangeRequestRawAddFileEntry;
use crate::domain::model::file::FileEntry;
use crate::domain::model::file_column_statistics::FileColumnStatistics;
use crate::domain::model::file_metadata::FileMetadata;

pub(crate) fn build_add_files_param(
    req: &AddFilesRequest,
) -> Result<AddFilesParam, ParameterError> {
    let table_identifier =
        to_table_identifier(required("table_identifier", req.table_identifier.as_ref())?)?;

    let idempotency_key = to_idempotency_key(req.idempotency_key.clone())?;

    let mut entries = vec![];
    for entry in req.add_file_entries.iter() {
        let partition_value = to_unvalidated_partition(entry.partition.as_ref())?;
        entries.push(ChangeRequestRawAddFileEntry::new(
            partition_value,
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
        table_identifier,
        stream: req.stream.into(),
        entries,
    };
    Ok(param)
}
