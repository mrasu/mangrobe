use crate::domain::model::user_table_id::UserTableId;
use crate::domain::model::user_table_name::UserTableName;
use crate::domain::service::user_table_service::UserTableService;
use crate::util::error::UserError;

pub async fn find_table_id(
    user_table_service: &UserTableService,
    name: &UserTableName,
) -> Result<UserTableId, anyhow::Error> {
    let table_id = user_table_service.find_id_by_name(name).await?;

    let Some(table_id) = table_id else {
        return Err(UserError::InvalidParameterMessage(format!(
            "table_name '{}' not found",
            name.val()
        ))
        .into());
    };

    Ok(table_id)
}
