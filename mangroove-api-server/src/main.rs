use crate::grpc::action_service::ActionService;
use crate::grpc::proto::action_service_server::ActionServiceServer;
use crate::grpc::proto::snapshot_service_server::SnapshotServiceServer;
use crate::grpc::snapshot_service::SnapshotService;
use crate::infrastructure::db::connection::connect;
use sea_orm::DatabaseConnection;
use tonic::transport::Server;

mod application;
mod domain;
mod grpc;
mod infrastructure;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .init();

    // TODO: omit
    let db = connect("postgres://postgres:@127.0.0.1:5432/mangroove-development".into()).await?;

    run_api_server(&db).await?;

    db.close().await?;

    Ok(())
}

async fn run_api_server(db: &DatabaseConnection) -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let snapshot_service = SnapshotService::new(db);
    let action_service = ActionService::new(db);

    Server::builder()
        .add_service(SnapshotServiceServer::new(snapshot_service))
        .add_service(ActionServiceServer::new(action_service))
        .serve(addr)
        .await?;

    Ok(())
}
