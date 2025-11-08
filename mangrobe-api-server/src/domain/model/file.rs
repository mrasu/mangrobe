use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct File {
    pub tenant_id: i64,
    pub partition_time: DateTime<Utc>,
    pub path: String,
    pub size: i64,
}

impl File {
    pub fn new(tenant_id: i64, partition_time: DateTime<Utc>, path: String, size: i64) -> Self {
        Self {
            tenant_id,
            partition_time,
            path,
            size,
        }
    }
}
