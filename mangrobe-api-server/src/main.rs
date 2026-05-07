use mangrobe_api_server::ApiServer;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::env;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

const DEFAULT_MANGROBE_API_ADDR: &str = "[::1]:50051";

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_env_filter(EnvFilter::new("info,sea_orm=debug"))
        .with_test_writer()
        .init();

    // TODO: omit
    let db = connect_db("postgres://postgres:@127.0.0.1:5432/mangrobe-development".into()).await?;

    let addr = env::var("MANGROBE_API_ADDR")
        .unwrap_or(DEFAULT_MANGROBE_API_ADDR.into())
        .parse()?;

    ApiServer::new(addr, db.clone()).run().await?;

    db.close().await?;

    Ok(())
}

async fn connect_db(url: String) -> Result<DatabaseConnection, anyhow::Error> {
    let mut opt = ConnectOptions::new(url);
    opt.max_connections(100)
        .min_connections(5)
        .connect_timeout(Duration::from_secs(8))
        .acquire_timeout(Duration::from_secs(8))
        .idle_timeout(Duration::from_secs(8))
        .max_lifetime(Duration::from_secs(8))
        .sqlx_logging(true)
        // TODO: log in production?
        .sqlx_logging_level(log::LevelFilter::Debug);
    let db = Database::connect(opt).await?;

    Ok(db)
}
