use crate::grpc::data_manipulation_service::DataManipulationService;
use crate::grpc::proto::FILE_DESCRIPTOR_SET2;
use crate::grpc::proto::data_manipulation_service_server::DataManipulationServiceServer;
use crate::infrastructure::db::connection::connect;
use sea_orm::DatabaseConnection;
use tonic::transport::Server;
use tonic_reflection::server::Builder;

mod application;
mod domain;
mod grpc;
mod infrastructure;
mod util;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .init();

    // TODO: omit
    let db = connect("postgres://postgres:@127.0.0.1:5432/mangrobe-development".into()).await?;

    run_api_server(&db).await?;

    db.close().await?;

    Ok(())
}

async fn run_api_server(db: &DatabaseConnection) -> Result<(), anyhow::Error> {
    let addr = "[::1]:50051".parse()?;

    let snapshot_service = DataManipulationService::new(db);

    Server::builder()
        .add_service(DataManipulationServiceServer::new(snapshot_service))
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
