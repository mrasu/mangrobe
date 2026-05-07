use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum ParameterError {
    #[error("Invalid parameter. key='{0}'. required")]
    Required(String),

    #[error("Invalid parameter. key='{0}', message='{1}'")]
    Invalid(String, String),
}
