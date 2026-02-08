use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct Stream {
    pub table_name: String,
    pub stream_id: i64,
}

impl Stream {
    pub fn new_with_random_stream_id(table_name: String) -> Result<Self, anyhow::Error> {
        let data = Self {
            table_name,
            stream_id: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs() as i64,
        };

        Ok(data)
    }
}
