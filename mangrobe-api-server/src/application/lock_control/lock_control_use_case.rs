use crate::application::lock_control::acquire_file_lock_param::AcquireFileLockParam;
use crate::application::util::user_table::find_table_id;
use crate::domain::model::file::FileWithId;
use crate::domain::model::file_lock_key::FileLockKey;
use crate::domain::model::user_table_stream::UserTablStream;
use crate::domain::service::file_lock_key_service::FileLockService;
use crate::domain::service::user_table_service::UserTableService;
use sea_orm::DatabaseConnection;

pub struct LockControlUseCase {
    file_lock_service: FileLockService,
    user_table_service: UserTableService,
}

impl LockControlUseCase {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            file_lock_service: FileLockService::new(&connection),
            user_table_service: UserTableService::new(&connection),
        }
    }

    pub async fn acquire_lock(
        &self,
        param: AcquireFileLockParam,
    ) -> Result<Vec<FileWithId>, anyhow::Error> {
        let table_id = find_table_id(&self.user_table_service, &param.table_name).await?;
        let stream = UserTablStream::new(table_id, param.stream_id);
        let locked_files = self
            .file_lock_service
            .acquire(&param.file_lock_key, &stream, param.ttl, &param.entries)
            .await?;

        Ok(locked_files)
    }

    pub async fn release_lock(&self, file_lock_key: FileLockKey) -> Result<bool, anyhow::Error> {
        let deleted = self.file_lock_service.release(&file_lock_key).await?;
        Ok(deleted)
    }
}
