use crate::domain::file::File;

#[derive(Default)]
pub struct Snapshot {
    pub files: Vec<File>,
}
