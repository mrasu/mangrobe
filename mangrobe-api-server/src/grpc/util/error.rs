use crate::util::error::ParameterError;
use tonic::{Code, Status};
use tracing::error;

pub fn to_internal_error(error: anyhow::Error) -> Status {
    error!(?error, "internal server error");
    Status::new(Code::Internal, "internal server error")
}

pub fn build_invalid_argument(err: ParameterError) -> Status {
    match err {
        ParameterError::Required(key) => build_argument_required(key),
        ParameterError::Invalid(key, msg) => build_invalid_argument_with_message(key, msg),
    }
}

fn build_invalid_argument_with_message(key: String, message: String) -> Status {
    Status::new(
        Code::InvalidArgument,
        format!("key: {}, message: {}", key, message),
    )
}

fn build_argument_required(key: String) -> Status {
    build_invalid_argument_with_message(key, "required".into())
}
