use crate::application::lock_control::acquire_file_lock_param::AcquireFileLockParam;
use crate::domain::model::file::FileWithId;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::service::file_lock_key_service::FileLockService;
use sea_orm::DatabaseConnection;

pub struct LockControlUseCase {
    file_lock_service: FileLockService,
}

impl LockControlUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            file_lock_service: FileLockService::new(&connection),
        }
    }

    pub async fn acquire_lock(
        &self,
        param: AcquireFileLockParam,
    ) -> Result<Vec<FileWithId>, anyhow::Error> {
        let locked_files = self
            .file_lock_service
            .acquire(
                &param.file_lock_key,
                &param.stream,
                param.ttl,
                &param.entries,
            )
            .await?;

        Ok(locked_files)
    }

    pub async fn release_lock(&self, file_lock_key: FileLockKey) -> Result<bool, anyhow::Error> {
        let deleted = self.file_lock_service.release(&file_lock_key).await?;
        Ok(deleted)
    }
}
