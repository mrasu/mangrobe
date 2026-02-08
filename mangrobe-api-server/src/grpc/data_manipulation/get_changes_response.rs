use crate::domain::model::change_request_file_data::FileData;
use crate::domain::model::committed_change_request::{
    ChangeRequestFileData, CommittedStreamChange,
};
use crate::domain::model::user_table_name::UserTableName;
use crate::grpc::proto::commit::Changes;
use crate::grpc::proto::{
    AddedFiles, ChangedFiles, Commit, CommittedFile, CompactedFile, CompactedFiles,
    GetCommitsResponse,
};

pub(crate) fn build_get_commits_response(
    table_name: &UserTableName,
    stream_changes: CommittedStreamChange,
) -> GetCommitsResponse {
    GetCommitsResponse {
        table_name: table_name.val(),
        stream_id: stream_changes.stream_id.val(),
        commits: stream_changes
            .committed_changes
            .iter()
            .map(|stream_change| Commit {
                commit_id: stream_change.commit_id.to_string(),
                changes: build_changes(&stream_change.file_data),
            })
            .collect(),
    }
}

fn build_changes(file_data: &ChangeRequestFileData) -> Option<Changes> {
    let changes = match file_data {
        ChangeRequestFileData::AddFiles { add_files } => Changes::AddedFiles(AddedFiles {
            added_files: build_committed_files(&add_files.files),
        }),
        ChangeRequestFileData::ChangeFiles { change_files } => {
            Changes::ChangedFiles(ChangedFiles {
                deleted_files: build_committed_files(&change_files.delete_files),
            })
        }
        ChangeRequestFileData::Compact { compact } => Changes::CompactedFiles(CompactedFiles {
            compacted_files: compact
                .compacted_files
                .iter()
                .map(|compact| CompactedFile {
                    src_files: build_committed_files(&compact.src_files),
                    dst_file: Some(build_committed_file(&compact.dst_file)),
                })
                .collect(),
        }),
    };

    Some(changes)
}

fn build_committed_files(files: &[FileData]) -> Vec<CommittedFile> {
    files.iter().map(build_committed_file).collect()
}

fn build_committed_file(file: &FileData) -> CommittedFile {
    CommittedFile {
        path: file.path.path(),
    }
}
