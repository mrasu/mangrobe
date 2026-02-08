use crate::application::data_manipulation::get_file_info_param::GetFileInfoParam;
use crate::domain::model::file::FileWithStatistics;
use crate::grpc::proto::{FileColumnStatistics, FileInfoResponse};

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

    FileInfoResponse {
        file_id: file_info.id.val().to_string(),
        path: file_info.file.path.path(),
        size: file_info.file.size,
        column_statistics,
    }
}
