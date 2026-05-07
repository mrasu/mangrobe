use crate::api::core::data_manipulation::add_files_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::AddFilesRequest;
use crate::application::data_manipulation::add_files_param::AddFilesParam;
use tonic::Request;

pub(super) fn build_add_files_param(
    request: Request<AddFilesRequest>,
) -> Result<AddFilesParam, ParameterError> {
    let req = request.get_ref();

    add_files_param::build_add_files_param(req)
}
