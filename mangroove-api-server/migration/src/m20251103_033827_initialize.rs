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
                    .table(ChangeRequest::Table)
                    .if_not_exists()
                    .col(
                        big_integer(ChangeRequest::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(string(ChangeRequest::IdempotencyKey).string_len(32))
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
                    .table(ChangeCommit::Table)
                    .if_not_exists()
                    .col(
                        big_integer(ChangeCommit::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(big_integer(ChangeCommit::ChangeRequestId))
                    .col(
                        timestamp_with_time_zone(ChangeCommit::CommittedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name("fk_change_requests_change_commits")
                    .from(ChangeCommit::Table, ChangeCommit::ChangeRequestId)
                    .to(ChangeRequest::Table, ChangeRequest::Id)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(ChangeRequestAddFile::Table)
                    .if_not_exists()
                    .col(
                        big_integer(ChangeRequestAddFile::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(big_integer(ChangeRequestAddFile::ChangeRequestId))
                    .col(string(ChangeRequestAddFile::Path))
                    .col(big_integer(ChangeRequestAddFile::Size))
                    .col(
                        timestamp_with_time_zone(ChangeRequestAddFile::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(ChangeRequestAddFile::UpdatedAt)
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
                    ChangeRequestAddFile::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name("fk_change_request_change_request_add_files")
                    .from(
                        ChangeRequestAddFile::Table,
                        ChangeRequestAddFile::ChangeRequestId,
                    )
                    .to(ChangeRequest::Table, ChangeRequest::Id)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ChangeRequestAddFile::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(ChangeCommit::Table).to_owned())
            .await
    }
}

// TODO: add tenant

#[derive(DeriveIden)]
enum ChangeRequest {
    #[sea_orm(iden = "change_requests")]
    Table,
    Id,
    IdempotencyKey,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum ChangeCommit {
    #[sea_orm(iden = "change_commits")]
    Table,
    Id,
    ChangeRequestId,
    CommittedAt,
}

#[derive(DeriveIden)]
enum ChangeRequestAddFile {
    #[sea_orm(iden = "change_request_add_files")]
    Table,
    Id,
    ChangeRequestId,
    Path,
    Size,
    CreatedAt,
    UpdatedAt,
}
