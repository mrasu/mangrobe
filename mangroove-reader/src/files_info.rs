use std::string::ToString;

pub(crate) struct FilesInfo {
    pub files: Vec<FileInfo>,
}

pub(crate) struct FileInfo {
    pub name: String,
    pub size: u64,
}

pub async fn get_files_info() -> FilesInfo {
    FilesInfo {
        files: vec![
            FileInfo {
                name: "example1.vortex".to_string(),
                size: 12572,
            },
            FileInfo {
                name: "example2.vortex".to_string(),
                size: 31388,
            },
        ],
    }
}
