use crate::domain::model::file::File;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::lock_raw_file_entry::LockFileRawAcquireEntry;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::infrastructure::db::repository::current_file_repository::CurrentFileRepository;
use crate::infrastructure::db::repository::file_lock_repository::FileLockRepository;
use crate::infrastructure::db::repository::file_repository::FileRepository;
use crate::util::error::UserError;
use anyhow::bail;
use chrono::Duration;
use sea_orm::{DatabaseConnection, TransactionTrait};

pub struct FileLockService {
    connection: DatabaseConnection,
    file_lock_repository: FileLockRepository,
    file_repository: FileRepository,
    current_file_repository: CurrentFileRepository,
}

impl FileLockService {
    pub fn new(connection: &DatabaseConnection) -> Self {
        Self {
            connection: connection.clone(),
            file_lock_repository: FileLockRepository::new(),
            file_repository: FileRepository::new(),
            current_file_repository: CurrentFileRepository::new(),
        }
    }

    pub async fn check_existence(
        &self,
        file_lock_key: &FileLockKey,
    ) -> Result<bool, anyhow::Error> {
        self.file_lock_repository
            .exists(&self.connection, file_lock_key)
            .await
    }

    pub async fn acquire(
        &self,
        file_lock_key: &FileLockKey,
        stream: &UserTablStream,
        ttl: Duration,
        entries: &[LockFileRawAcquireEntry],
    ) -> Result<Vec<File>, anyhow::Error> {
        let txn = self.connection.begin().await?;

        let acquired = self
            .file_lock_repository
            .acquire(&txn, stream, ttl, file_lock_key)
            .await?;

        if !acquired {
            bail!(UserError::InvalidLockMessage(
                "cannot acquired. duplicated?".into()
            ));
        }

        let mut file_ids = vec![];
        for entry in entries {
            let locked_files = self
                .current_file_repository
                .select_files_by_paths_for_update(
                    &txn,
                    stream,
                    entry.partition_time,
                    &entry.file_paths,
                )
                .await?;

            if locked_files.len() != entry.file_paths.len() {
                bail!(UserError::InvalidLockMessage("some files not found".into()))
            }
            file_ids.extend(locked_files.iter().map(|f| f.file_id.clone()));
        }

        let locked_count = self
            .current_file_repository
            .acquire_lock(&txn, file_lock_key, stream, &file_ids)
            .await?;

        if locked_count as usize != file_ids.len() {
            bail!(UserError::InvalidLockMessage(
                "not all files can be locked".into()
            ))
        }

        let files = self
            .file_repository
            .find_all_by_ids(&txn, stream, &file_ids)
            .await?;

        txn.commit().await?;

        Ok(files)
    }
}
