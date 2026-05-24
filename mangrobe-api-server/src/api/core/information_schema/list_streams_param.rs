use crate::api::core::information_schema::list_stream_page_token::ListStreamPageToken;
use crate::api::core::util::error::ParameterError;
use crate::api::core::util::page::build_page;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::{ListStreamsRequest, PaginationRequest};
use crate::application::information_schema::list_streams_param::ListStreamsParam;

const DEFAULT_PAGE_SIZE: i32 = 1000;

pub(crate) fn parse_list_streams_param(
    req: &ListStreamsRequest,
) -> Result<(ListStreamsParam, i32), ParameterError> {
    let table_identifier =
        to_table_identifier(required("table_identifier", req.table_identifier.as_ref())?)?;

    let pagination = req.pagination.clone().unwrap_or(PaginationRequest {
        size: 0,
        token: None,
    });
    let page = build_page(&pagination, DEFAULT_PAGE_SIZE)?;

    let stream_after = match page.token {
        Some(token) => {
            let token = ListStreamPageToken::parse(token).ok_or(invalid_page_token())?;
            if token.table_identifier != table_identifier {
                return Err(invalid_page_token());
            }
            Some(token.stream)
        }
        None => None,
    };

    Ok((
        ListStreamsParam {
            table_identifier,
            stream_after,
        },
        page.size,
    ))
}

fn invalid_page_token() -> ParameterError {
    ParameterError::Invalid("page_token".to_string(), "invalid".to_string())
}
