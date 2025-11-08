use crate::domain::model::change_request_compact_file_entry::FilePath;
use crate::domain::model::file::File;
use crate::domain::model::file_id::FileId;
use crate::infrastructure::db::entity::files;
use crate::infrastructure::db::entity::prelude::Files;
use chrono::{DateTime, Utc};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Set};

pub struct FileRepository {}

impl FileRepository {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn find_all_by_id<C>(
        &self,
        con: &C,
        file_ids: Vec<FileId>,
    ) -> Result<Vec<File>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let ids: Vec<i64> = file_ids.iter().map(|id| id.val()).collect();
        let files = Files::find()
            .filter(files::Column::Id.is_in(ids))
            .all(con)
            .await?;

        let res = files
            .iter()
            .map(|f| File {
                tenant_id: f.tenant_id,
                partition_time: f.partition_time.into(),
                path: f.path.clone(),
                size: f.size,
            })
            .collect();

        Ok(res)
    }

    pub async fn find_all_ids_by_locator<C>(
        &self,
        con: &C,
        tenant_id: i64,
        partition_time: DateTime<Utc>,
        file_paths: &[FilePath],
    ) -> Result<Vec<FileId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let file_ids: Vec<FileId> = Files::find()
            .select_only()
            .column(files::Column::Id)
            .filter(files::Column::TenantId.eq(tenant_id))
            .filter(files::Column::PartitionTime.eq(partition_time))
            .filter(files::Column::Path.is_in(file_paths.iter().map(|f| f.path.clone())))
            .into_tuple::<i64>()
            .all(con)
            .await?
            .iter()
            .map(|i| (*i).into())
            .collect();

        Ok(file_ids)
    }

    pub async fn insert<C>(&self, con: &C, target_file: &File) -> Result<FileId, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let file = Self::new_active_model(target_file);
        let inserted = Files::insert(file).exec_with_returning(con).await?;

        Ok(inserted.id.into())
    }

    pub async fn insert_many<C>(
        &self,
        con: &C,
        target_files: &[File],
    ) -> Result<Vec<FileId>, anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let files = target_files.iter().map(Self::new_active_model);

        let inserted = Files::insert_many(files)
            .exec_with_returning_many(con)
            .await?;

        let file_ids: Vec<FileId> = inserted.iter().map(|i| i.id.into()).collect();
        Ok(file_ids)
    }

    fn new_active_model(file: &File) -> files::ActiveModel {
        files::ActiveModel {
            tenant_id: Set(file.tenant_id),
            partition_time: Set(file.partition_time.into()),
            path: Set(file.path.clone()),
            size: Set(file.size),
            ..Default::default()
        }
    }
}
