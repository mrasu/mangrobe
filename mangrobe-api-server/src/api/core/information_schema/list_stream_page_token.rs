use crate::domain::model::stream_id::StreamId;
use crate::domain::model::table_identifier::TableIdentifier;

pub(super) struct ListStreamPageToken {
    pub(super) table_identifier: TableIdentifier,
    pub(super) stream_id: StreamId,
}

impl ListStreamPageToken {
    pub(super) fn new(table_identifier: TableIdentifier, stream_id: StreamId) -> Self {
        Self {
            table_identifier,
            stream_id,
        }
    }

    pub(super) fn parse(token: String) -> Option<ListStreamPageToken> {
        let mut parts = token.split(':');
        let token_catalog_name = parts.next()?;
        let token_schema_name = parts.next()?;
        let token_table_name = parts.next()?;
        let token_stream_id = parts.next()?;

        if parts.next().is_some() {
            return None;
        }

        let table_identifier = TableIdentifier::new(
            token_catalog_name.to_string().try_into().ok()?,
            token_schema_name.to_string().try_into().ok()?,
            token_table_name.to_string().try_into().ok()?,
        );

        let token_stream_id: i64 = token_stream_id.parse().ok()?;

        Some(ListStreamPageToken {
            table_identifier,
            stream_id: token_stream_id.into(),
        })
    }

    pub(super) fn to_token_string(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.table_identifier.catalog_name.val(),
            self.table_identifier.schema_name.val(),
            self.table_identifier.table_name.val(),
            self.stream_id.val()
        )
    }
}
