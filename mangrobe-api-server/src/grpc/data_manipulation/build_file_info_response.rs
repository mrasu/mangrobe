use crate::application::data_manipulation::get_file_info_param::GetFileInfoParam;
use crate::domain::model::file_with_statistics::FileWithStatistics;
use crate::grpc::proto::{
    FileColumnStatistics, FileInfoResponse, FileMetadata as FileMetadataResponse,
};

pub(crate) fn build_file_info_response(
    request_param: &GetFileInfoParam,
    file_info: &FileWithStatistics,
) -> FileInfoResponse {
    let column_statistics = file_info
        .column_statistics
        .iter()
        .map(|file| {
            let mut stat = FileColumnStatistics {
                column_name: file.column_name.clone(),
                min: None,
                max: None,
            };
            if request_param.stat_types.includes_min {
                stat.min = file.min
            }
            if request_param.stat_types.includes_max {
                stat.max = file.max
            }

            stat
        })
        .collect();

    let mut file_metadata = FileMetadataResponse {
        parquet_metadata: None,
    };
    if request_param.metadata_types.includes_parquet_metadata {
        file_metadata.parquet_metadata = file_info
            .file_metadata
            .as_ref()
            .and_then(|metadata| metadata.parquet_metadata.clone());
    }

    FileInfoResponse {
        file_id: file_info.id.val().to_string(),
        path: file_info.file.path.path(),
        size: file_info.file.size,
        column_statistics,
        file_metadata: Some(file_metadata),
    }
}
