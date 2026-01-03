use crate::domain::model::stream_id::StreamId;
use crate::domain::model::user_table_id::UserTableId;

pub(super) struct ListStreamPageToken {
    pub(super) table_id: UserTableId,
    pub(super) stream_id: StreamId,
}

impl ListStreamPageToken {
    pub(super) fn new(table_id: UserTableId, stream_id: StreamId) -> Self {
        Self {
            table_id,
            stream_id,
        }
    }

    pub(super) fn parse(token: String) -> Option<ListStreamPageToken> {
        let mut parts = token.split(':');
        let token_table_id = parts.next()?;
        let token_stream_id = parts.next()?;

        if parts.next().is_some() {
            return None;
        }

        let token_table_id: i64 = token_table_id.parse().ok()?;
        let token_stream_id: i64 = token_stream_id.parse().ok()?;

        Some(ListStreamPageToken {
            table_id: token_table_id.into(),
            stream_id: token_stream_id.into(),
        })
    }

    pub(super) fn to_token_string(&self) -> String {
        format!("{}:{}", self.table_id.val(), self.stream_id.val())
    }
}
