use crate::api::core::util::error::ParameterError;

pub(crate) fn required<'a, T>(key: &str, value: Option<&'a T>) -> Result<&'a T, ParameterError> {
    value.ok_or_else(|| ParameterError::Required(key.to_owned()))
}

pub(crate) fn invalid_enum<T>(key: &str) -> Result<T, ParameterError> {
    Err(ParameterError::Invalid(
        key.to_owned(),
        "unsupported or unspecified enum value".to_owned(),
    ))
}
