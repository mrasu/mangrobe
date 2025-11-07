mod grpc;
mod infrastructure;
mod prepare;
mod vortex_provider;

use crate::grpc::api_client::ApiClient;
use crate::infrastructure::s3::store::create_rustfs;
use crate::prepare::prepare;
use crate::vortex_provider::VortexProvider;
use clap::{Parser, Subcommand};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    subcommand: SubCommands,
}

#[derive(Debug, Subcommand)]
enum SubCommands {
    Serve {
        #[command(subcommand)]
        command: ServeCommands,
    },
    Prepare,
}

#[derive(Debug, Subcommand)]
enum ServeCommands {
    Reader,
    Writer,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.subcommand {
        SubCommands::Serve { command } => match command {
            ServeCommands::Reader => {
                // TODO: serve
                run_query().await.unwrap();
            }
            ServeCommands::Writer => {
                // TODO: write more or serve
                prepare().await.unwrap();
            }
        },
        SubCommands::Prepare => {
            prepare().await.unwrap();
        }
    }
}

async fn run_query() -> Result<(), anyhow::Error> {
    let ctx = SessionContext::default().enable_url_table();

    let conn = tonic::transport::Endpoint::new("http://[::1]:50051")?
        .connect()
        .await?;
    let api_client = ApiClient::new(conn);

    let object_store_url = ObjectStoreUrl::parse("s3://mangroove-development")?;
    let rustfs = create_rustfs()?;
    ctx.register_object_store(object_store_url.as_ref(), Arc::new(rustfs));

    let provider = VortexProvider::new(api_client, &object_store_url)?;
    ctx.register_table("custom_vortex_table", Arc::new(provider))?;

    run_df_query(
        &ctx,
        "SELECT * FROM custom_vortex_table order by id limit 3",
    )
    .await?;
    run_df_query(
        &ctx,
        "SELECT * FROM custom_vortex_table order by id desc limit 3",
    )
    .await?;

    Ok(())
}

async fn run_df_query(ctx: &SessionContext, query: &str) -> Result<(), anyhow::Error> {
    let res = ctx.sql(query).await?;
    res.show().await?;

    Ok(())
}
