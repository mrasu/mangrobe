use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct Stream {
    pub table_id: i64,
    pub stream_id: i64,
}

impl Stream {
    pub fn new_with_random_stream_id(table_id: i64) -> Result<Self, anyhow::Error> {
        let data = Self {
            table_id,
            stream_id: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs() as i64,
        };

        Ok(data)
    }
}
