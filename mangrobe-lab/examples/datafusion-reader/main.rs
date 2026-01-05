mod prepare;
mod vortex_provider;

use crate::prepare::{prepare_table, register_files};
use crate::vortex_provider::VortexProvider;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use mangrobe_lab::{ApiClient, Stream, create_rustfs};
use std::env;
use std::sync::Arc;

const DEFAULT_MANGROBE_API_ADDR: &str = "http://[::1]:50051";
const BUCKET_NAME: &str = "mangrobe-lab-datafusion-reader";
const QUERY_TABLE_ID: i64 = 902;

#[tokio::main]
async fn main() {
    println!("Running datafusion-reader...");

    let api_server_addr = env::var("MANGROBE_API_ADDR").unwrap_or(DEFAULT_MANGROBE_API_ADDR.into());
    run(api_server_addr).await.unwrap();
}

async fn run(api_server_addr: String) -> Result<(), anyhow::Error> {
    let stream = prepare_table(BUCKET_NAME.into()).await?;
    register_files(api_server_addr.clone(), &stream, BUCKET_NAME.into()).await?;

    query_datafusion(api_server_addr, stream).await?;

    Ok(())
}

async fn query_datafusion(api_server_addr: String, stream: Stream) -> Result<(), anyhow::Error> {
    let ctx = build_datafusion_ctx(api_server_addr, stream).await?;

    let sql = "SELECT * FROM custom_vortex_table order by id limit 3";
    println!("Running: {}", sql);
    let res = ctx.sql(sql).await?;
    res.show().await?;

    let sql = "SELECT * FROM custom_vortex_table order by id desc limit 3";
    println!("Running: {}", sql);
    let res = ctx.sql(sql).await?;
    res.show().await?;

    Ok(())
}

async fn build_datafusion_ctx(
    api_server_addr: String,
    stream: Stream,
) -> Result<SessionContext, anyhow::Error> {
    let ctx = SessionContext::default().enable_url_table();

    let conn = tonic::transport::Endpoint::new(api_server_addr)?
        .connect()
        .await?;
    let api_client = ApiClient::new(conn);

    let rustfs = create_rustfs(BUCKET_NAME.into())?;
    let object_store_url = ObjectStoreUrl::parse(format!("s3://{}", BUCKET_NAME))?;
    ctx.register_object_store(object_store_url.as_ref(), Arc::new(rustfs));

    let provider = VortexProvider::new(api_client, &object_store_url, stream)?;
    ctx.register_table("custom_vortex_table", Arc::new(provider))?;

    Ok(ctx)
}
