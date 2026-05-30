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
use tonic::service::Routes;
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

        let routes = MangrobeGrpcServices::builder(self.db.clone())
            .enable_reflection()
            .build()?;

        Server::builder()
            .add_routes(routes)
            .serve(self.addr)
            .await?;

        Ok(())
    }
}

pub struct MangrobeGrpcServices;

impl MangrobeGrpcServices {
    pub fn builder(db: DatabaseConnection) -> MangrobeGrpcServicesBuilder {
        MangrobeGrpcServicesBuilder {
            db,
            reflection_v1: false,
            reflection_v1alpha: false,
        }
    }
}

pub struct MangrobeGrpcServicesBuilder {
    db: DatabaseConnection,
    reflection_v1: bool,
    reflection_v1alpha: bool,
}

impl MangrobeGrpcServicesBuilder {
    pub fn enable_reflection(self) -> Self {
        self.reflection_variables(true, true)
    }

    pub fn enable_reflection_v1(self) -> Self {
        let v1alpha = self.reflection_v1alpha;
        self.reflection_variables(true, v1alpha)
    }

    pub fn enable_reflection_v1alpha(self) -> Self {
        let v1 = self.reflection_v1;
        self.reflection_variables(v1, true)
    }

    fn reflection_variables(mut self, v1: bool, v1alpha: bool) -> Self {
        self.reflection_v1 = v1;
        self.reflection_v1alpha = v1alpha;
        self
    }

    pub fn build(self) -> Result<Routes, tonic_reflection::server::Error> {
        let data_manipulation_service = DataManipulationService::new(self.db.clone());
        let data_definition_service = DataDefinitionService::new(self.db.clone());
        let lock_control_service = LockControlService::new(self.db.clone());
        let information_schema_service = InformationSchemaService::new(self.db);

        let mut builder = Routes::builder();
        builder
            .add_service(DataManipulationServiceServer::new(
                data_manipulation_service,
            ))
            .add_service(DataDefinitionServiceServer::new(data_definition_service))
            .add_service(LockControlServiceServer::new(lock_control_service))
            .add_service(InformationSchemaServiceServer::new(
                information_schema_service,
            ));

        if self.reflection_v1alpha {
            builder.add_service(
                Builder::configure()
                    .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET2)
                    .build_v1alpha()?,
            );
        }

        if self.reflection_v1 {
            builder.add_service(
                Builder::configure()
                    .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET2)
                    .build_v1()?,
            );
        }

        Ok(builder.routes())
    }
}
