use crate::domain::model::stream::Stream;
use crate::domain::model::table_identifier::TableIdentifier;

pub(super) struct ListStreamPageToken {
    pub(super) table_identifier: TableIdentifier,
    pub(super) stream: Stream,
}

impl ListStreamPageToken {
    pub(super) fn new(table_identifier: TableIdentifier, stream: Stream) -> Self {
        Self {
            table_identifier,
            stream,
        }
    }

    pub(super) fn parse(token: String) -> Option<ListStreamPageToken> {
        let mut parts = token.split(':');
        let token_catalog_name = parts.next()?;
        let token_schema_name = parts.next()?;
        let token_table_name = parts.next()?;
        let token_stream = parts.next()?;

        if parts.next().is_some() {
            return None;
        }

        let table_identifier = TableIdentifier::new(
            token_catalog_name.to_string().try_into().ok()?,
            token_schema_name.to_string().try_into().ok()?,
            token_table_name.to_string().try_into().ok()?,
        );

        let token_stream: i64 = token_stream.parse().ok()?;

        Some(ListStreamPageToken {
            table_identifier,
            stream: token_stream.into(),
        })
    }

    pub(super) fn to_token_string(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.table_identifier.catalog_name.val(),
            self.table_identifier.schema_name.val(),
            self.table_identifier.table_name.val(),
            self.stream.val()
        )
    }
}
