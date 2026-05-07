#[derive(Clone, Debug)]
pub(crate) struct FileMetadata {
    pub parquet_metadata: Option<Vec<u8>>,
}

impl FileMetadata {
    pub fn new(parquet_metadata: Option<Vec<u8>>) -> Self {
        Self { parquet_metadata }
    }
}
