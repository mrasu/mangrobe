use chrono::{DateTime, Utc};

#[derive(Clone)]
pub struct FileLockKey {
    pub key: Vec<u8>,
    pub request_started_at: DateTime<Utc>,
}

impl FileLockKey {
    pub fn new(key: Vec<u8>, request_started_at: DateTime<Utc>) -> Result<Self, String> {
        if key.is_empty() || key.len() > 16 {
            return Err("length must be 1 to 16".into());
        }

        Ok(Self {
            key,
            request_started_at,
        })
    }
}
