#[derive(Debug)]
pub struct ChangeRequestChangeEntries {
    pub add_entries: Vec<ChangeRequestFileAddEntry>,
}

impl ChangeRequestChangeEntries {
    pub fn new(add_entries: Vec<ChangeRequestFileAddEntry>) -> Self {
        Self { add_entries }
    }
}

#[derive(Debug)]
pub struct ChangeRequestFileAddEntry {
    pub path: String,
    pub size: i64,
}

impl ChangeRequestFileAddEntry {
    pub fn new(path: String, size: i64) -> Self {
        Self { path, size }
    }
}
