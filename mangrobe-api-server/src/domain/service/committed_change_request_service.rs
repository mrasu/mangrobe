use crate::domain::model::commit_id::CommitId;
use crate::domain::model::committed_change_request::{
    CommittedChangeRequest, CommittedChangeRequestData,
};
use crate::domain::model::file::FileWithId;
use crate::domain::model::file_id::FileId;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::repository::commit_repository::CommitRepository;
use crate::infrastructure::db::repository::file_repository::FileRepository;
use sea_orm::{
    AccessMode, DatabaseConnection, DatabaseTransaction, IsolationLevel, TransactionTrait,
};
use std::collections::HashMap;

#[derive(Clone)]
pub struct CommittedChangeRequestService {
    connection: DatabaseConnection,
    commit_repository: CommitRepository,
    file_repository: FileRepository,
}

impl CommittedChangeRequestService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            commit_repository: CommitRepository::new(),
            file_repository: FileRepository::new(),
        }
    }

    pub async fn get_after(
        &self,
        stream: &UserTablStream,
        commit_id: &CommitId,
        limit_per_stream: u64,
    ) -> Result<Vec<CommittedChangeRequestData>, anyhow::Error> {
        let txn = self
            .connection
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await?;

        let committed_change_requests = self
            .commit_repository
            .find_change_requests_after(&txn, stream, commit_id, limit_per_stream)
            .await?;

        let change_request_data = self
            .convert_raw_committed_change_request_to_data(&txn, stream, &committed_change_requests)
            .await?;

        Ok(change_request_data)
    }

    async fn convert_raw_committed_change_request_to_data(
        &self,
        txn: &DatabaseTransaction,
        stream: &UserTablStream,
        committed_change_requests: &[CommittedChangeRequest],
    ) -> Result<Vec<CommittedChangeRequestData>, anyhow::Error> {
        let file_ids: Vec<FileId> = committed_change_requests
            .iter()
            .flat_map(|req| req.file_entry.file_ids())
            .collect();

        let files = self
            .file_repository
            .find_all_by_ids(txn, stream, &file_ids)
            .await?;

        let file_map: HashMap<FileId, FileWithId> = files
            .into_iter()
            .map(|f| (f.id.clone(), f.clone()))
            .collect();

        let change_request_data = committed_change_requests
            .iter()
            .map(|req| CommittedChangeRequestData {
                commit_id: req.commit_id.clone(),
                file_data: req.file_entry.to_file_data(&file_map),
            })
            .collect();

        Ok(change_request_data)
    }
}
