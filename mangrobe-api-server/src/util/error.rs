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

    #[error("Invalid parameter. key='{0}', value='{1}'")]
    Invalid(String, String),
}
