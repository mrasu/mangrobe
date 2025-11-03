pub struct ChangedFiles {
    pub added_files: Vec<AddedFile>,
}

impl ChangedFiles {
    pub fn new(added_files: Vec<AddedFile>) -> Self {
        Self { added_files }
    }
}

pub struct AddedFile {
    pub path: String,
    pub size: i64,
}

impl AddedFile {
    pub fn new(path: String, size: i64) -> Self {
        Self { path, size }
    }
}
