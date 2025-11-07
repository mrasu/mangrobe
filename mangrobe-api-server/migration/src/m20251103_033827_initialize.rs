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
                    .table(ChangeRequestFileAddEntry::Table)
                    .if_not_exists()
                    .col(
                        big_integer(ChangeRequestFileAddEntry::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(big_integer(ChangeRequestFileAddEntry::ChangeRequestId))
                    .col(string(ChangeRequestFileAddEntry::Path))
                    .col(big_integer(ChangeRequestFileAddEntry::Size))
                    .col(
                        timestamp_with_time_zone(ChangeRequestFileAddEntry::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(ChangeRequestFileAddEntry::UpdatedAt)
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
                    ChangeRequestFileAddEntry::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        manager
            .create_index(
                Index::create()
                    .name(format!(
                        "idx_{}_{}",
                        ChangeRequestFileAddEntry::Table.to_string(),
                        ChangeRequestFileAddEntry::ChangeRequestId.to_string()
                    ))
                    .table(ChangeRequestFileAddEntry::Table)
                    .col(ChangeRequestFileAddEntry::ChangeRequestId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name(format!(
                        "fk_{}_{}",
                        ChangeRequestFileAddEntry::Table.to_string(),
                        ChangeRequest::Table.to_string()
                    ))
                    .from(
                        ChangeRequestFileAddEntry::Table,
                        ChangeRequestFileAddEntry::ChangeRequestId,
                    )
                    .to(ChangeRequest::Table, ChangeRequest::Id)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(ChangeRequestFileAddEntry::Table)
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
enum ChangeRequestFileAddEntry {
    #[sea_orm(iden = "change_request_file_add_entries")]
    Table,
    Id,
    ChangeRequestId,
    Path,
    Size,
    CreatedAt,
    UpdatedAt,
}
