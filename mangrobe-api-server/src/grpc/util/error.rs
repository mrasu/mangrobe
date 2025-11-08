use tonic::{Code, Status};
use tracing::error;

pub fn to_internal_error(error: anyhow::Error) -> Status {
    error!(?error, "internal server error");
    Status::new(Code::Internal, "internal server error")
}

pub fn build_invalid_argument(key: &str, message: &str) -> Status {
    Status::new(
        Code::InvalidArgument,
        format!("key: {}, message: {}", key, message),
    )
}

pub fn build_argument_required(key: &str) -> Status {
    build_invalid_argument(key, "required")
}
