use crate::sea_orm::Statement;
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute(Statement::from_string(
                manager.get_database_backend(),
                r#"
                CREATE OR REPLACE FUNCTION update_timestamp()
                RETURNS TRIGGER AS $$
                BEGIN
                  NEW.updated_at = NOW();
                  RETURN NEW;
                END;
                $$ language 'plpgsql';
                "#
                .to_owned(),
            ))
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(UserTable::Table)
                    .if_not_exists()
                    .col(
                        big_integer(UserTable::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(
                        timestamp_with_time_zone(UserTable::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(UserTable::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute(Statement::from_string(
                manager.get_database_backend(),
                format!(
                    r#"
                CREATE TRIGGER trigger_update_updated_at
                BEFORE UPDATE ON {}
                FOR EACH ROW
                EXECUTE FUNCTION update_timestamp();
                "#,
                    UserTable::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(ChangeRequest::Table)
                    .if_not_exists()
                    .col(
                        big_integer(ChangeRequest::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(big_integer(ChangeRequest::UserTableId))
                    .col(big_integer(ChangeRequest::StreamId))
                    .col(timestamp_with_time_zone(ChangeRequest::PartitionTime))
                    .col(integer(ChangeRequest::Status))
                    .col(integer(ChangeRequest::ChangeType))
                    .col(json_binary_null(ChangeRequest::FileEntry))
                    .col(
                        timestamp_with_time_zone(ChangeRequest::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(ChangeRequest::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute(Statement::from_string(
                manager.get_database_backend(),
                format!(
                    r#"
                CREATE TRIGGER trigger_update_updated_at
                BEFORE UPDATE ON {}
                FOR EACH ROW
                EXECUTE FUNCTION update_timestamp();
                "#,
                    ChangeRequest::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        // TODO: add FK after adding rpc for table creation
        //
        // manager
        //     .create_foreign_key(
        //         ForeignKey::create()
        //             .name(format!(
        //                 "fk_{}_{}",
        //                 ChangeRequest::Table.to_string(),
        //                 UserTable::Table.to_string()
        //             ))
        //             .from(ChangeRequest::Table, ChangeRequest::UserTableId)
        //             .to(UserTable::Table, UserTable::Id)
        //             .to_owned(),
        //     )
        //     .await?;

        manager
            .create_table(
                Table::create()
                    .table(ChangeRequestIdempotencyKey::Table)
                    .if_not_exists()
                    .col(binary_len(ChangeRequestIdempotencyKey::Key, 16).primary_key())
                    .col(big_integer(ChangeRequestIdempotencyKey::ChangeRequestId).unique_key())
                    .col(
                        timestamp_with_time_zone(ChangeRequestIdempotencyKey::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(ChangeRequestIdempotencyKey::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(ChangeRequestIdempotencyKey::ExpiresAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name(format!(
                        "idx_{}_{}",
                        ChangeRequestIdempotencyKey::Table.to_string(),
                        ChangeRequestIdempotencyKey::ChangeRequestId.to_string()
                    ))
                    .table(ChangeRequestIdempotencyKey::Table)
                    .col(ChangeRequestIdempotencyKey::ChangeRequestId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name(format!(
                        "fk_{}_{}",
                        ChangeRequestIdempotencyKey::Table.to_string(),
                        ChangeRequest::Table.to_string()
                    ))
                    .from(
                        ChangeRequestIdempotencyKey::Table,
                        ChangeRequestIdempotencyKey::ChangeRequestId,
                    )
                    .to(ChangeRequest::Table, ChangeRequest::Id)
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute(Statement::from_string(
                manager.get_database_backend(),
                format!(
                    r#"
                CREATE TRIGGER trigger_update_updated_at
                BEFORE UPDATE ON {}
                FOR EACH ROW
                EXECUTE FUNCTION update_timestamp();
                "#,
                    ChangeRequestIdempotencyKey::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Commit::Table)
                    .if_not_exists()
                    .col(
                        big_integer(Commit::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(big_integer(Commit::ChangeRequestId))
                    .col(big_integer(Commit::UserTableId))
                    .col(big_integer(Commit::StreamId))
                    .col(
                        timestamp_with_time_zone(Commit::CommittedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name(format!(
                        "idx_{}_{}",
                        Commit::Table.to_string(),
                        Commit::ChangeRequestId.to_string()
                    ))
                    .table(Commit::Table)
                    .col(Commit::ChangeRequestId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name(format!(
                        "idx_{}_{}_{}",
                        Commit::Table.to_string(),
                        Commit::UserTableId.to_string(),
                        Commit::StreamId.to_string()
                    ))
                    .table(Commit::Table)
                    .col(Commit::UserTableId)
                    .col(Commit::StreamId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name(format!(
                        "fk_{}_{}",
                        Commit::Table.to_string(),
                        ChangeRequest::Table.to_string()
                    ))
                    .from(Commit::Table, Commit::ChangeRequestId)
                    .to(ChangeRequest::Table, ChangeRequest::Id)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(File::Table)
                    .if_not_exists()
                    .col(big_integer(File::Id).auto_increment().primary_key().take())
                    .col(big_integer(File::UserTableId))
                    .col(big_integer(File::StreamId))
                    .col(timestamp_with_time_zone(File::PartitionTime))
                    .col(string_len(File::Path, 255))
                    .col(binary_len(File::PathXxh3, 16))
                    .col(big_integer(File::Size))
                    .col(
                        timestamp_with_time_zone(File::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(File::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute(Statement::from_string(
                manager.get_database_backend(),
                format!(
                    r#"
                CREATE TRIGGER trigger_update_updated_at
                BEFORE UPDATE ON {}
                FOR EACH ROW
                EXECUTE FUNCTION update_timestamp();
                "#,
                    File::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        manager
            .create_index(
                Index::create()
                    .name(format!(
                        "idx_{}_{}_{}_{}",
                        File::Table.to_string(),
                        File::StreamId.to_string(),
                        File::PartitionTime.to_string(),
                        File::PathXxh3.to_string(),
                    ))
                    .table(File::Table)
                    .col(File::StreamId)
                    .col(File::PartitionTime)
                    .col(File::PathXxh3)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(FileLock::Table)
                    .if_not_exists()
                    .col(binary_len(FileLock::Key, 16).primary_key())
                    .col(big_integer(FileLock::UserTableId))
                    .col(big_integer(FileLock::StreamId))
                    .col(timestamp_with_time_zone(FileLock::PartitionTime))
                    .col(timestamp_with_time_zone(FileLock::ExpireAt))
                    .col(
                        timestamp_with_time_zone(FileLock::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(FileLock::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute(Statement::from_string(
                manager.get_database_backend(),
                format!(
                    r#"
                CREATE TRIGGER trigger_update_updated_at
                BEFORE UPDATE ON {}
                FOR EACH ROW
                EXECUTE FUNCTION update_timestamp();
                "#,
                    FileLock::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(CurrentFile::Table)
                    .if_not_exists()
                    .col(
                        big_integer(CurrentFile::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(big_integer(CurrentFile::UserTableId))
                    .col(big_integer(CurrentFile::StreamId))
                    .col(timestamp_with_time_zone(CurrentFile::PartitionTime))
                    .col(big_integer(CurrentFile::FileId))
                    .col(binary_len(CurrentFile::FilePathXxh3, 16))
                    .col(binary_len_null(CurrentFile::FileLockKey, 16))
                    .col(
                        timestamp_with_time_zone(CurrentFile::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(CurrentFile::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute(Statement::from_string(
                manager.get_database_backend(),
                format!(
                    r#"
                CREATE TRIGGER trigger_update_updated_at
                BEFORE UPDATE ON {}
                FOR EACH ROW
                EXECUTE FUNCTION update_timestamp();
                "#,
                    CurrentFile::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name(format!(
                        "fk_{}_{}",
                        CurrentFile::Table.to_string(),
                        File::Table.to_string()
                    ))
                    .from(CurrentFile::Table, CurrentFile::FileId)
                    .to(File::Table, File::Id)
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name(format!(
                        "fk_{}_{}",
                        CurrentFile::Table.to_string(),
                        FileLock::Table.to_string()
                    ))
                    .from(CurrentFile::Table, CurrentFile::FileLockKey)
                    .to(FileLock::Table, FileLock::Key)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name(format!(
                        "idx_{}_{}_{}_{}_{}",
                        CurrentFile::Table.to_string(),
                        CurrentFile::UserTableId.to_string(),
                        CurrentFile::StreamId.to_string(),
                        CurrentFile::PartitionTime.to_string(),
                        CurrentFile::FilePathXxh3.to_string()
                    ))
                    .unique()
                    .table(CurrentFile::Table)
                    .col(CurrentFile::UserTableId)
                    .col(CurrentFile::StreamId)
                    .col(CurrentFile::PartitionTime)
                    .col(CurrentFile::FilePathXxh3)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name(format!(
                        "idx_{}_{}",
                        CurrentFile::Table.to_string(),
                        CurrentFile::FileLockKey.to_string(),
                    ))
                    .table(CurrentFile::Table)
                    .col(CurrentFile::FileLockKey)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(CurrentFile::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(FileLock::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(File::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(Commit::Table).to_owned())
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(ChangeRequestIdempotencyKey::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(Table::drop().table(ChangeRequest::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(UserTable::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum UserTable {
    #[sea_orm(iden = "user_tables")]
    Table,
    Id,
    // TODO: add definition's info
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum ChangeRequest {
    #[sea_orm(iden = "change_requests")]
    Table,
    Id,
    UserTableId,
    StreamId,
    PartitionTime,
    Status,
    ChangeType,
    FileEntry,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum ChangeRequestIdempotencyKey {
    #[sea_orm(iden = "change_request_idempotency_keys")]
    Table,
    Key,
    ChangeRequestId,
    CreatedAt,
    UpdatedAt,
    ExpiresAt,
}

#[derive(DeriveIden)]
enum Commit {
    #[sea_orm(iden = "commits")]
    Table,
    Id,
    ChangeRequestId,
    UserTableId,
    StreamId,
    CommittedAt,
}

#[derive(DeriveIden)]
enum File {
    #[sea_orm(iden = "files")]
    Table,
    Id,
    UserTableId,
    StreamId,
    PartitionTime,
    Path,
    PathXxh3,
    Size,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum CurrentFile {
    #[sea_orm(iden = "current_files")]
    Table,
    Id,
    UserTableId,
    StreamId,
    PartitionTime,
    FileId,
    FilePathXxh3,
    FileLockKey,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum FileLock {
    #[sea_orm(iden = "file_locks")]
    Table,
    Key,
    UserTableId,
    StreamId,
    PartitionTime,
    ExpireAt,
    CreatedAt,
    UpdatedAt,
}
