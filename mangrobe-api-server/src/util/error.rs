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
#[allow(clippy::enum_variant_names)]
pub(crate) enum UserError {
    #[error("Invalid parameter. {0}")]
    InvalidParameterMessage(String),

    #[error("Invalid lock. {0}")]
    InvalidLockMessage(String),

    #[error("Already exists. {0}")]
    AlreadyExistsMessage(String),

    #[error("Not found. {0}")]
    NotFoundMessage(String),
}
