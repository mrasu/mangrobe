use crate::sea_orm::{DeriveActiveEnum, EnumIter, Iterable, Statement};
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
                    .table(ChangeRequest::Table)
                    .if_not_exists()
                    .col(
                        big_integer(ChangeRequest::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(big_integer(ChangeRequest::TenantId))
                    .col(timestamp_with_time_zone(ChangeRequest::PartitionTime))
                    .col(integer(ChangeRequest::Status))
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
                    .table(ChangeRequestFileEntry::Table)
                    .if_not_exists()
                    .col(
                        big_integer(ChangeRequestFileEntry::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(big_integer(ChangeRequestFileEntry::ChangeRequestId))
                    .col(integer(ChangeRequestFileEntry::ChangeType))
                    .col(json_binary(ChangeRequestFileEntry::ChangeEntries))
                    .col(
                        timestamp_with_time_zone(ChangeRequestFileEntry::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(ChangeRequestFileEntry::UpdatedAt)
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
                    ChangeRequestFileEntry::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        manager
            .create_index(
                Index::create()
                    .name(format!(
                        "idx_{}_{}",
                        ChangeRequestFileEntry::Table.to_string(),
                        ChangeRequestFileEntry::ChangeRequestId.to_string()
                    ))
                    .table(ChangeRequestFileEntry::Table)
                    .col(ChangeRequestFileEntry::ChangeRequestId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name(format!(
                        "fk_{}_{}",
                        ChangeRequestFileEntry::Table.to_string(),
                        ChangeRequest::Table.to_string()
                    ))
                    .from(
                        ChangeRequestFileEntry::Table,
                        ChangeRequestFileEntry::ChangeRequestId,
                    )
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
                    .col(big_integer(File::TenantId))
                    .col(timestamp_with_time_zone(File::PartitionTime))
                    .col(string_len(File::Path, 100))
                    .col(big_integer(File::Size))
                    .col(
                        timestamp_with_time_zone(ChangeRequestFileEntry::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(ChangeRequestFileEntry::UpdatedAt)
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
                        "idx_{}_{}_{}",
                        File::Table.to_string(),
                        File::TenantId.to_string(),
                        File::PartitionTime.to_string()
                    ))
                    .table(File::Table)
                    .col(File::TenantId)
                    .col(File::PartitionTime)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(File::Table).to_owned())
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(ChangeRequestFileEntry::Table)
                    .to_owned(),
            )
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
            .await
    }
}

#[derive(DeriveIden)]
enum ChangeRequest {
    #[sea_orm(iden = "change_requests")]
    Table,
    Id,
    TenantId,
    PartitionTime,
    Status,
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
    CommittedAt,
}

#[derive(DeriveIden)]
enum ChangeRequestFileEntry {
    #[sea_orm(iden = "change_request_file_entries")]
    Table,
    Id,
    ChangeRequestId,
    ChangeType,
    ChangeEntries,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum File {
    #[sea_orm(iden = "files")]
    Table,
    Id,
    TenantId,
    PartitionTime,
    Path,
    Size,
    CreatedAt,
    UpdatedAt,
}
