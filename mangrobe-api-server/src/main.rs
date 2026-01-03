use crate::grpc::data_manipulation::data_manipulation_service::DataManipulationService;
use crate::grpc::information_schema::information_schema_service::InformationSchemaService;
use crate::grpc::lock_control::lock_control_service::LockControlService;
use crate::grpc::proto::FILE_DESCRIPTOR_SET2;
use crate::grpc::proto::data_manipulation_service_server::DataManipulationServiceServer;
use crate::grpc::proto::information_schema_service_server::InformationSchemaServiceServer;
use crate::grpc::proto::lock_control_service_server::LockControlServiceServer;
use crate::infrastructure::db::connection::connect;
use sea_orm::DatabaseConnection;
use std::convert::Into;
use std::env;
use std::net::SocketAddr;
use tonic::transport::Server;
use tonic_reflection::server::Builder;
use tracing_subscriber::EnvFilter;

mod application;
mod domain;
mod grpc;
mod infrastructure;
mod util;

const DEFAULT_MANGROBE_API_ADDR: &str = "[::1]:50051";

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_env_filter(EnvFilter::new("info,sea_orm=debug"))
        .with_test_writer()
        .init();

    // TODO: omit
    let db = connect("postgres://postgres:@127.0.0.1:5432/mangrobe-development".into()).await?;

    let addr = env::var("MANGROBE_API_ADDR")
        .unwrap_or(DEFAULT_MANGROBE_API_ADDR.into())
        .parse()?;

    run_api_server(addr, &db).await?;

    db.close().await?;

    Ok(())
}

async fn run_api_server(addr: SocketAddr, db: &DatabaseConnection) -> Result<(), anyhow::Error> {
    println!("Starting Mangrobe API Server at {}...", addr);

    let snapshot_service = DataManipulationService::new(db);
    let lock_control_service = LockControlService::new(db);
    let information_schema_service = InformationSchemaService::new(db);

    Server::builder()
        .add_service(DataManipulationServiceServer::new(snapshot_service))
        .add_service(LockControlServiceServer::new(lock_control_service))
        .add_service(InformationSchemaServiceServer::new(
            information_schema_service,
        ))
        .add_service(
            Builder::configure()
                .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET2)
                .build_v1alpha()?,
        )
        .add_service(
            Builder::configure()
                .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET2)
                .build_v1()?,
        )
        .serve(addr)
        .await?;

    Ok(())
}
