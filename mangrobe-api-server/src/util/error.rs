use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum MangrobeError {
    #[error("Unexpected state found. {0}")]
    UnexpectedState(String),

    #[error("Unexpected state change found. from {0} to {1}")]
    UnexpectedStateChange(String, String),
}

#[derive(Error, Debug)]
pub(crate) enum UserError {
    #[error("Invalid parameter. {0}")]
    InvalidParameterMessage(String),

    #[error("Invalid lock. {0}")]
    InvalidLockMessage(String),

    #[error("Already exists. {0}")]
    AlreadyExistsMessage(String),
}
