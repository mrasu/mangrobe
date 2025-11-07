use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MangobeError {
    #[error("Unexpected state found. {0}")]
    UnexpectedState(String),

    #[error("Unexpected state change found. from {0} to {1}")]
    UnexpectedStateChange(String, String),
}
