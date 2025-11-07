use crate::domain::model::file::File;

#[derive(Default)]
pub struct Snapshot {
    pub files: Vec<File>,
}
