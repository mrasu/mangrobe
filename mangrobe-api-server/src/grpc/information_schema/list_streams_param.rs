use crate::application::information_schema::list_streams_param::ListStreamsParam;
use crate::grpc::information_schema::list_stream_page_token::ListStreamPageToken;
use crate::grpc::model::page::build_page;
use crate::grpc::proto::{ListStreamsRequest, PaginationRequest};
use crate::grpc::util::param_util::to_table_name;
use crate::util::error::ParameterError;
use tonic::Request;

const DEFAULT_PAGE_SIZE: i32 = 1000;

pub(super) fn parse_list_streams_param(
    request: Request<ListStreamsRequest>,
) -> Result<(ListStreamsParam, i32), ParameterError> {
    let req = request.get_ref();
    let table_name = to_table_name(req.table_name.clone())?;

    let pagination = req.pagination.clone().unwrap_or(PaginationRequest {
        size: 0,
        token: "".to_string(),
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
