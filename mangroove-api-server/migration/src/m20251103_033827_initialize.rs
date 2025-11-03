use crate::sea_orm::Statement;
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(DeltaLog::Table)
                    .if_not_exists()
                    .col(
                        big_integer(DeltaLog::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(
                        timestamp_with_time_zone(DeltaLog::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(DeltaFile::Table)
                    .if_not_exists()
                    .col(
                        big_integer(DeltaFile::Id)
                            .auto_increment()
                            .primary_key()
                            .take(),
                    )
                    .col(big_integer(DeltaFile::DeltaLogId))
                    .col(string(DeltaFile::Path))
                    .col(big_integer(DeltaFile::Size))
                    .col(
                        timestamp_with_time_zone(DeltaFile::CreatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone(DeltaFile::UpdatedAt)
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

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
                    DeltaFile::Table.to_string()
                )
                .to_owned(),
            ))
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .name("fk_delta_log_delta_file")
                    .from(DeltaFile::Table, DeltaFile::DeltaLogId)
                    .to(DeltaLog::Table, DeltaLog::Id)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(DeltaFile::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(DeltaLog::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum DeltaLog {
    #[sea_orm(iden = "delta_logs")]
    Table,
    Id,
    CreatedAt,
}

#[derive(DeriveIden)]
enum DeltaFile {
    #[sea_orm(iden = "delta_files")]
    Table,
    Id,
    DeltaLogId,
    Path,
    Size,
    CreatedAt,
    UpdatedAt,
}
