use crate::domain::model::stream::Stream;
use crate::domain::model::user_table_name::UserTableName;
use crate::grpc::information_schema::list_stream_page_token::ListStreamPageToken;
use crate::grpc::proto::{ListStreamsResponse, PaginationResponse, StreamInfo};

pub(super) fn build_list_streams_response(
    table_name: &UserTableName,
    page_size: usize,
    streams: &[Stream],
) -> ListStreamsResponse {
    let pagination = if streams.len() > page_size {
        let last_stream = &streams[page_size - 1];
        let next_token = ListStreamPageToken::new(table_name.clone(), last_stream.id.clone());

        Some(PaginationResponse {
            next_token: next_token.to_token_string(),
        })
    } else {
        None
    };

    ListStreamsResponse {
        table_name: table_name.val(),
        streams: streams
            .iter()
            .take(page_size)
            .map(|stream| StreamInfo {
                stream_id: stream.id.val(),
                last_commit_id: stream.last_commit_id.to_string(),
            })
            .collect(),
        pagination,
    }
}
