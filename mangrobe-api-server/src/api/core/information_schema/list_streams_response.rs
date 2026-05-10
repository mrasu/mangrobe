use crate::api::core::information_schema::list_stream_page_token::ListStreamPageToken;
use crate::api::core::util::param::table_identifier::to_proto_table_identifier;
use crate::api::grpc::proto::{ListStreamsResponse, PaginationResponse, StreamInfo};
use crate::domain::model::stream::Stream;
use crate::domain::model::table_identifier::TableIdentifier;

pub(crate) fn build_list_streams_response(
    table_identifier: &TableIdentifier,
    page_size: usize,
    streams: &[Stream],
) -> ListStreamsResponse {
    let pagination = if streams.len() > page_size {
        let last_stream = &streams[page_size - 1];
        let next_token = ListStreamPageToken::new(table_identifier.clone(), last_stream.id.clone());

        Some(PaginationResponse {
            next_token: next_token.to_token_string(),
        })
    } else {
        None
    };

    ListStreamsResponse {
        table_identifier: Some(to_proto_table_identifier(table_identifier.clone())),
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
