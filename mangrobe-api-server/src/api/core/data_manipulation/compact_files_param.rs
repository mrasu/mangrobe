use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::file_lock_key::to_file_lock_key;
use crate::api::core::util::param::partition::to_unvalidated_partition;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::CompactFilesRequest;
use crate::application::data_manipulation::compact_files_param::CompactFilesParam;
use crate::domain::model::change_request_raw_file_entry::{
    ChangeRequestRawCompactFileInfoEntry, ChangeRequestRawCompactFilesEntry,
};
use crate::domain::model::file::FileEntry;
use crate::domain::model::file_column_statistics::FileColumnStatistics;
use crate::domain::model::file_metadata::FileMetadata;
use chrono::{DateTime, Utc};

pub(crate) fn build_compact_files_param(
    req: &CompactFilesRequest,
    request_started_at: DateTime<Utc>,
) -> Result<CompactFilesParam, ParameterError> {
    let table_identifier =
        to_table_identifier(required("table_identifier", req.table_identifier.as_ref())?)?;

    let file_lock_key = to_file_lock_key(req.file_lock_key.clone(), request_started_at)?;

    let mut entries = vec![];
    for entry in req.compact_file_entries.iter() {
        let partition = to_unvalidated_partition(entry.partition.as_ref())?;

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

            let mut dst_stats = Vec::with_capacity(req_dst_file.column_statistics.len());
            for statistics in &req_dst_file.column_statistics {
                if statistics.column_name.is_empty() {
                    return Err(ParameterError::Required("column_name".to_string()));
                }
                dst_stats.push(FileColumnStatistics::new(
                    statistics.column_name.clone(),
                    statistics.min,
                    statistics.max,
                ));
            }
            let dst_file = FileEntry::new(
                req_dst_file.path.clone().into(),
                req_dst_file.size,
                dst_stats,
                req_dst_file
                    .file_metadata
                    .as_ref()
                    .map(|metadata| FileMetadata::new(metadata.parquet_metadata.clone())),
            );
            file_info_entries.push(ChangeRequestRawCompactFileInfoEntry::new(
                src_file_paths,
                dst_file,
            ))
        }
        entries.push(ChangeRequestRawCompactFilesEntry::new(
            partition,
            file_info_entries,
        ))
    }

    let param = CompactFilesParam {
        file_lock_key,
        table_identifier,
        stream: req.stream.into(),
        entries,
    };
    Ok(param)
}
