use tonic::{Code, Status};
use tracing::error;

pub fn to_internal_error(error: Box<dyn std::error::Error>) -> Status {
    error!(?error, "internal server error");
    Status::new(Code::Internal, "internal server error")
}
