use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MangrobeError {
    #[error("Unexpected state found. {0}")]
    UnexpectedState(String),

    #[error("Unexpected state change found. from {0} to {1}")]
    UnexpectedStateChange(String, String),
}

#[derive(Error, Debug)]
pub enum ParameterError {
    #[error("Invalid parameter. key='{0}'. required")]
    Required(String),

    #[error("Invalid parameter. key='{0}', message='{1}'")]
    Invalid(String, String),
}

#[derive(Error, Debug)]
pub enum UserError {
    #[error("Invalid parameter. {0}")]
    InvalidParameterMessage(String),

    #[error("Invalid lock. {0}")]
    InvalidLockMessage(String),
}
