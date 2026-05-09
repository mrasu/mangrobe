use crate::api::core::util::error::ParameterError;
use crate::domain::model::user_table_name::UserTableName;

pub(crate) fn to_table_name(table_name: String) -> Result<UserTableName, ParameterError> {
    match table_name.try_into() {
        Ok(t) => Ok(t),
        Err(e) => Err(ParameterError::Invalid("table_name".into(), e)),
    }
}
