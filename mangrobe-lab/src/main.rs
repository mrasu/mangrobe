mod grpc;
mod infrastructure;
mod prepare;
mod prometheus;

use crate::grpc::api_client::ApiClient;
use crate::infrastructure::s3::store::create_rustfs;
use crate::prepare::smoke_run;
use crate::prometheus::handler::Handler;
use clap::{Parser, Subcommand};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, StatusCode};
use prost_types::Timestamp;
use std::convert::Infallible;

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
    SmokeRun,
}

#[derive(Debug, Subcommand)]
enum ServeCommands {
    Writer,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.subcommand {
        SubCommands::Serve { command } => match command {
            ServeCommands::Writer => {
                serve_writer().await.unwrap();
            }
        },
        SubCommands::SmokeRun => {
            smoke_run().await.unwrap();
        }
    }
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

    let rustfs = create_rustfs("mangrobe-development".into())?;

    let conn = tonic::transport::Endpoint::new("http://[::1]:50051")?
        .connect()
        .await?;
    let api_client = ApiClient::new(conn);

    let handler = Handler::new(rustfs, api_client);
    handler.handle_remote_write(req).await
}
