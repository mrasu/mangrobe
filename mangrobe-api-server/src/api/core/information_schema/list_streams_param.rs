use crate::api::core::information_schema::list_stream_page_token::ListStreamPageToken;
use crate::api::core::util::error::ParameterError;
use crate::api::core::util::page::build_page;
use crate::api::core::util::param_util::to_table_name;
use crate::api::grpc::proto::{ListStreamsRequest, PaginationRequest};
use crate::application::information_schema::list_streams_param::ListStreamsParam;

const DEFAULT_PAGE_SIZE: i32 = 1000;

pub(crate) fn parse_list_streams_param(
    req: &ListStreamsRequest,
) -> Result<(ListStreamsParam, i32), ParameterError> {
    let table_name = to_table_name(req.table_name.clone())?;

    let pagination = req.pagination.clone().unwrap_or(PaginationRequest {
        size: 0,
        token: None,
    });
    let page = build_page(&pagination, DEFAULT_PAGE_SIZE)?;

    let stream_id_after = match page.token {
        Some(token) => {
            let token = ListStreamPageToken::parse(token).ok_or(invalid_page_token())?;
            if token.table_name != table_name {
                return Err(invalid_page_token());
            }
            Some(token.stream_id)
        }
        None => None,
    };

    Ok((
        ListStreamsParam {
            table_name,
            stream_id_after,
        },
        page.size,
    ))
}

fn invalid_page_token() -> ParameterError {
    ParameterError::Invalid("page_token".to_string(), "invalid".to_string())
}
