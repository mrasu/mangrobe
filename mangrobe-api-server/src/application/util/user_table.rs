use crate::domain::model::table_identifier::TableIdentifier;
use crate::domain::model::user_table_id::UserTableId;
use crate::domain::service::user_table_service::UserTableService;
use crate::util::error::UserError;

pub async fn find_table_id(
    user_table_service: &UserTableService,
    identifier: &TableIdentifier,
) -> Result<UserTableId, anyhow::Error> {
    let table_id = user_table_service.find_id_by_identifier(identifier).await?;

    let Some(table_id) = table_id else {
        return Err(UserError::InvalidParameterMessage(format!(
            "table '{}' not found",
            identifier.full_name()
        ))
        .into());
    };

    Ok(table_id)
}
