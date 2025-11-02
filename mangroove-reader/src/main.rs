mod files_info;
mod prepare;
mod vortex_provider;

use crate::prepare::prepare;
use crate::vortex_provider::VortexProvider;
use arrow_array::RecordBatchReader;
use arrow_array::cast::AsArray;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use std::path::PathBuf;
use std::sync::Arc;
use std::{env, fs};
use vortex;
use vortex::Array;
use vortex::dtype::arrow::FromArrowType;
use vortex_array::arrow::FromArrowArray;
use vortex_array::stream::ArrayStreamExt;
use vortex_array::{ArrayVisitor, IntoArray, ToCanonical};

#[tokio::main]
async fn main() {
    let dir = &data_dir().unwrap();
    if !dir.exists() {
        fs::create_dir(dir).unwrap();
        prepare(dir).await.unwrap();
    }
    run_query(dir).await.unwrap();
}

fn data_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let d = env::current_dir()?.join("mangroove-reader").join("data");
    Ok(d)
}

async fn run_query(target_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::default().enable_url_table();

    let object_store_url = ObjectStoreUrl::parse("file://")?;
    let provider = VortexProvider::new(&object_store_url, target_dir)?;
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

async fn run_df_query(ctx: &SessionContext, query: &str) -> Result<(), Box<dyn std::error::Error>> {
    let res = ctx.sql(query).await?;
    res.show().await?;

    Ok(())
}
