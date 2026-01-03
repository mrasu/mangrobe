mod grpc;
mod infrastructure;
mod prepare;
mod prometheus;
mod vortex_provider;

use crate::grpc::api_client::ApiClient;
use crate::infrastructure::s3::store::create_rustfs;
use crate::prepare::{prepare, smoke_run};
use crate::prometheus::handler::Handler;
use crate::vortex_provider::VortexProvider;
use clap::{Parser, Subcommand};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, StatusCode};
use prost_types::Timestamp;
use std::convert::Infallible;
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
    SmokeRun,
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
                serve_writer().await.unwrap();
            }
        },
        SubCommands::Prepare => {
            prepare().await.unwrap();
        }
        SubCommands::SmokeRun => {
            smoke_run().await.unwrap();
        }
    }
}

async fn run_query() -> Result<(), anyhow::Error> {
    let ctx = SessionContext::default().enable_url_table();

    let conn = tonic::transport::Endpoint::new("http://[::1]:50051")?
        .connect()
        .await?;
    let api_client = ApiClient::new(conn);

    let object_store_url = ObjectStoreUrl::parse("s3://mangrobe-development")?;
    let rustfs = create_rustfs()?;
    ctx.register_object_store(object_store_url.as_ref(), Arc::new(rustfs));

    let provider = VortexProvider::new(api_client.clone(), &object_store_url)?;
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

    let prom_provider =
        crate::prometheus::provider::Provider::new(api_client.clone(), &object_store_url)?;
    ctx.register_table("prometheus_table", Arc::new(prom_provider))?;

    run_df_query(
        &ctx,
        "SELECT timeseries.labels[1]['value'] AS label_value, timeseries.labels[1]['name'] as label_name, timeseries.samples[1]['value'] AS value FROM prometheus_table limit 3",
    )
        .await?;
    run_df_query(
        &ctx,
        "SELECT * FROM prometheus_table order by timeseries.samples[1]['timestamp'] desc limit 3",
    )
    .await?;

    Ok(())
}

async fn run_df_query(ctx: &SessionContext, query: &str) -> Result<(), anyhow::Error> {
    let res = ctx.sql(query).await?;
    res.show().await?;

    Ok(())
}

async fn serve_writer() -> Result<(), anyhow::Error> {
    let addr = ([0, 0, 0, 0], 8888).into();
    let make_svc = make_service_fn(move |_conn| async move {
        Ok::<_, Infallible>(service_fn(move |req| async move {
            match handle_remote_write(req).await {
                Ok(resp) => Ok::<_, Infallible>(resp),
                Err(err) => {
                    eprintln!("remote write handler error: {err:?}");
                    let mut resp = Response::new(Body::from("internal error"));
                    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(resp)
                }
            }
        }))
    });

    println!("starting remote write receiver on {addr}");
    hyper::Server::bind(&addr).serve(make_svc).await?;

    Ok(())
}

const DEFAULT_PARTITION_TIME: Timestamp = Timestamp {
    seconds: 0,
    nanos: 0,
};

async fn handle_remote_write(req: Request<Body>) -> Result<Response<Body>, anyhow::Error> {
    if req.method() != Method::POST || req.uri().path() != "/api/v1/write" {
        let mut resp = Response::new(Body::from("not found"));
        *resp.status_mut() = StatusCode::NOT_FOUND;
        return Ok(resp);
    }

    let rustfs = create_rustfs()?;

    let conn = tonic::transport::Endpoint::new("http://[::1]:50051")?
        .connect()
        .await?;
    let api_client = ApiClient::new(conn);

    let handler = Handler::new(rustfs, api_client);
    handler.handle_remote_write(req).await
}
