mod prometheus;

use crate::prometheus::handler::Handler;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, StatusCode};
use mangrobe_lab::{ApiClient, create_bucket_if_not_exists, create_rustfs};
use prost_types::Timestamp;
use std::convert::Infallible;
use std::env;

const DEFAULT_MANGROBE_API_ADDR: &str = "http://[::1]:50051";

pub const PROM_TABLE_ID: i64 = 903;
pub const PROM_STREAM_ID: i64 = 1;
const HTTP_SERVER_PORT: u16 = 8888;

#[tokio::main]
async fn main() {
    let api_server_addr = env::var("MANGROBE_API_ADDR").unwrap_or(DEFAULT_MANGROBE_API_ADDR.into());

    create_bucket_if_not_exists("mangrobe-development".into())
        .await
        .unwrap();
    serve_writer(api_server_addr).await.unwrap();
}

async fn serve_writer(api_server_addr: String) -> Result<(), anyhow::Error> {
    let conn = tonic::transport::Endpoint::new(api_server_addr)?
        .connect()
        .await?;
    let api_client = ApiClient::new(conn);

    let make_svc = make_service_fn(move |_conn| {
        let api_client = api_client.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let api_client = api_client.clone();
                async move {
                    match handle_remote_write(req, api_client).await {
                        Ok(resp) => Ok::<_, Infallible>(resp),
                        Err(err) => {
                            eprintln!("remote write handler error: {err:?}");
                            let mut resp = Response::new(Body::from("internal error"));
                            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            Ok(resp)
                        }
                    }
                }
            }))
        }
    });

    let addr = ([0, 0, 0, 0], HTTP_SERVER_PORT).into();
    println!("starting remote write receiver on {addr}");
    hyper::Server::bind(&addr).serve(make_svc).await?;

    Ok(())
}

const DEFAULT_PARTITION_TIME: Timestamp = Timestamp {
    seconds: 0,
    nanos: 0,
};

async fn handle_remote_write(
    req: Request<Body>,
    api_client: ApiClient,
) -> Result<Response<Body>, anyhow::Error> {
    if req.method() != Method::POST || req.uri().path() != "/api/v1/write" {
        let mut resp = Response::new(Body::from("not found"));
        *resp.status_mut() = StatusCode::NOT_FOUND;
        return Ok(resp);
    }

    let rustfs = create_rustfs("mangrobe-development".into())?;

    let handler = Handler::new(rustfs, api_client);
    handler.handle_remote_write(req).await
}
