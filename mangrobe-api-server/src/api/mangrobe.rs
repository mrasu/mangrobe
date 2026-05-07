use crate::api::core::data_definition::data_definition_service::DataDefinitionService;
use crate::api::core::data_manipulation::data_manipulation_service::DataManipulationService;
use crate::api::core::information_schema::information_schema_service::InformationSchemaService;
use crate::api::core::lock_control::lock_control_service::LockControlService;
use sea_orm::DatabaseConnection;

// referenced from lib.rs
pub struct Mangrobe {
    data_manipulation: DataManipulationService,
    data_definition: DataDefinitionService,
    lock_control: LockControlService,
    information_schema: InformationSchemaService,
}

impl Mangrobe {
    pub fn new_with_connection(db: DatabaseConnection) -> Self {
        Self {
            data_manipulation: DataManipulationService::new(db.clone()),
            data_definition: DataDefinitionService::new(db.clone()),
            lock_control: LockControlService::new(db.clone()),
            information_schema: InformationSchemaService::new(db.clone()),
        }
    }

    pub fn data_manipulation(&self) -> &DataManipulationService {
        &self.data_manipulation
    }

    pub fn data_definition(&self) -> &DataDefinitionService {
        &self.data_definition
    }

    pub fn lock_control(&self) -> &LockControlService {
        &self.lock_control
    }

    pub fn information_schema(&self) -> &InformationSchemaService {
        &self.information_schema
    }
}
