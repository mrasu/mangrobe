use crate::api::grpc::data_definition::data_definition_service::DataDefinitionService;
use crate::api::grpc::data_manipulation::data_manipulation_service::DataManipulationService;
use crate::api::grpc::information_schema::information_schema_service::InformationSchemaService;
use crate::api::grpc::lock_control::lock_control_service::LockControlService;
use crate::api::grpc::proto::FILE_DESCRIPTOR_SET2;
use crate::api::grpc::proto::data_definition_service_server::DataDefinitionServiceServer;
use crate::api::grpc::proto::data_manipulation_service_server::DataManipulationServiceServer;
use crate::api::grpc::proto::information_schema_service_server::InformationSchemaServiceServer;
use crate::api::grpc::proto::lock_control_service_server::LockControlServiceServer;
use sea_orm::DatabaseConnection;
use std::net::SocketAddr;
use tonic::transport::Server;
use tonic_reflection::server::Builder;

pub struct ApiServer {
    addr: SocketAddr,
    db: DatabaseConnection,
}

impl ApiServer {
    pub fn new(addr: SocketAddr, db: DatabaseConnection) -> Self {
        Self { addr, db }
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        println!("Starting Mangrobe API Server at {}...", self.addr);

        let data_manipulation_service = DataManipulationService::new(&self.db);
        let data_definition_service = DataDefinitionService::new(&self.db);
        let lock_control_service = LockControlService::new(&self.db);
        let information_schema_service = InformationSchemaService::new(&self.db);

        Server::builder()
            .add_service(DataManipulationServiceServer::new(
                data_manipulation_service,
            ))
            .add_service(DataDefinitionServiceServer::new(data_definition_service))
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
            .serve(self.addr)
            .await?;

        Ok(())
    }
}
