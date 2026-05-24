use crate::proto::{ExternalLocation, StorageScheme, TableIdentifier};
use std::time::SystemTime;

const DEFAULT_CATALOG_NAME: &str = "mangrobe_lab";
const DEFAULT_SCHEMA_NAME: &str = "default";

#[derive(Debug, Clone)]
pub struct Stream {
    pub table_identifier: TableIdentifier,
    pub location: ExternalLocation,
    pub stream: i64,
}

impl Stream {
    pub fn new_with_random_stream(
        table_name: String,
        bucket: String,
    ) -> Result<Self, anyhow::Error> {
        let data = Self {
            table_identifier: TableIdentifier {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                table_name,
            },
            location: ExternalLocation {
                storage_scheme: StorageScheme::S3.into(),
                bucket: Some(bucket),
                prefix: None,
                endpoint: None,
                region: None,
            },
            stream: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs() as i64,
        };

        Ok(data)
    }
}
