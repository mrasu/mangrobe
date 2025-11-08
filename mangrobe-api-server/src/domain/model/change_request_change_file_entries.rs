use crate::domain::model::file::File;

#[derive(Debug)]
pub struct ChangeRequestChangeFileEntries {
    pub add_entries: Vec<File>,
}

impl ChangeRequestChangeFileEntries {
    pub fn new(add_entries: Vec<File>) -> Self {
        Self { add_entries }
    }
}
