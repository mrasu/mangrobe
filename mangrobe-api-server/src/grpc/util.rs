use tonic::{Code, Status};
use tracing::error;

pub fn to_internal_error(error: anyhow::Error) -> Status {
    error!(?error, "internal server error");
    Status::new(Code::Internal, "internal server error")
}
