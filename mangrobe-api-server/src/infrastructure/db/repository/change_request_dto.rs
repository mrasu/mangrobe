use crate::domain::model::change_request::{
    BaseChangeRequest, ChangeRequest, ChangeRequestStatus, ChangeRequestType,
};
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::entity::change_requests::{ActiveModel, Model};
use crate::infrastructure::db::entity_ext::change_request_ext::ChangeRequestExt;
use sea_orm::Set;

pub(super) fn build_entity_change_request(
    stream: &UserTablStream,
    change_type: ChangeRequestType,
) -> ActiveModel {
    ActiveModel {
        id: Default::default(),
        stream_id: Set(stream.stream_id.val()),
        user_table_id: Set(stream.user_table_id.val()),
        status: Set(ChangeRequestExt::build_model_status(
            ChangeRequestStatus::New,
        )),
        change_type: Set(ChangeRequestExt::build_model_change_type(change_type)),
        file_entry: Set(None),
        created_at: Default::default(),
        updated_at: Default::default(),
    }
}

pub(super) fn build_domain_change_request(
    change_request: &Model,
) -> Result<ChangeRequest, anyhow::Error> {
    Ok(ChangeRequest {
        base: BaseChangeRequest {
            id: change_request.id.into(),
            stream: UserTablStream::new(
                change_request.user_table_id.into(),
                change_request.stream_id.into(),
            ),
            status: ChangeRequestExt::build_domain_status(change_request)?,
            change_type: ChangeRequestExt::build_domain_change_type(change_request)?,
        },
        file_entry: ChangeRequestExt::build_domain_file_entry(change_request)?,
    })
}
