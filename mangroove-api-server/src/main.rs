use crate::grpc::proto::snapshot_service_server::SnapshotServiceServer;
use crate::grpc::snapshot_service::SnapshotService;
use tonic::transport::Server;

mod grpc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let snapshot_service = SnapshotService::default();

    Server::builder()
        .add_service(SnapshotServiceServer::new(snapshot_service))
        .serve(addr)
        .await?;

    Ok(())
}
