use crate::application::data_manipulation::get_file_info_param::{
    FileMetadataSelection, FileStatisticsSelection, GetFileInfoParam,
};
use crate::domain::model::file_id::FileId;
use crate::grpc::proto::{FileColumnStatisticsType, FileMetadataType, GetFileInfoRequest};
use crate::util::error::ParameterError;
use tonic::Request;

pub(super) fn build_get_file_info_param(
    request: Request<GetFileInfoRequest>,
) -> Result<GetFileInfoParam, ParameterError> {
    let req = request.get_ref();

    let mut file_ids = Vec::with_capacity(req.file_ids.len());
    for raw_id in &req.file_ids {
        let file_id = raw_id.parse::<i64>().map_err(|_| {
            ParameterError::Invalid("file_ids".to_string(), "invalid number".to_string())
        })?;
        file_ids.push(FileId::from(file_id));
    }

    let mut include_min = false;
    let mut include_max = false;
    for stat_type in &req.included_column_statistics_types {
        match FileColumnStatisticsType::try_from(*stat_type) {
            Ok(FileColumnStatisticsType::Min) => include_min = true,
            Ok(FileColumnStatisticsType::Max) => include_max = true,

            Ok(FileColumnStatisticsType::Unspecified) => {
                return Err(ParameterError::Invalid(
                    "included_column_statistics_types".to_string(),
                    "unspecified is included".to_string(),
                ));
            }
            Err(_) => {
                return Err(ParameterError::Invalid(
                    "included_column_statistics_types".to_string(),
                    "unknown value".to_string(),
                ));
            }
        }
    }

    let mut include_parquet_metadata = false;
    for metadata_type in &req.included_file_metadata_types {
        match FileMetadataType::try_from(*metadata_type) {
            Ok(FileMetadataType::ParquetMetadata) => include_parquet_metadata = true,

            Ok(FileMetadataType::Unspecified) => {
                return Err(ParameterError::Invalid(
                    "included_file_metadata_types".to_string(),
                    "unspecified is included".to_string(),
                ));
            }
            Err(_) => {
                return Err(ParameterError::Invalid(
                    "included_file_metadata_types".to_string(),
                    "unknown value".to_string(),
                ));
            }
        }
    }

    Ok(GetFileInfoParam {
        file_ids,
        stat_types: FileStatisticsSelection::new(include_min, include_max),
        metadata_types: FileMetadataSelection::new(include_parquet_metadata),
    })
}
