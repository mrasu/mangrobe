use crate::api::core::lock_control::acquire_file_lock_param::build_acquire_file_lock_param as build_api_acquire_file_lock_param;
use crate::api::core::util::error::ParameterError;
use crate::api::grpc::proto::AcquireFileLockRequest;
use crate::application::lock_control::acquire_file_lock_param::AcquireFileLockParam;
use chrono::{DateTime, Utc};
use tonic::Request;

pub fn build_acquire_file_lock_param(
    request: Request<AcquireFileLockRequest>,
    request_started_at: DateTime<Utc>,
) -> Result<AcquireFileLockParam, ParameterError> {
    let req = request.get_ref();
    build_api_acquire_file_lock_param(req, request_started_at)
}
