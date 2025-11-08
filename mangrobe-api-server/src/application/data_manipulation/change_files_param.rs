use crate::application::data_manipulation::param_util::{to_idempotency_key, to_partition_time};
use crate::domain::model::change_request_change_file_entries::ChangeRequestChangeFileEntries;
use crate::domain::model::file::File;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::grpc::proto::ChangeFilesRequest;
use crate::util::error::ParameterError;
use chrono::{DateTime, Utc};

pub struct ChangeFilesParam {
    pub idempotency_key: IdempotencyKey,
    pub tenant_id: i64,
    pub partition_time: DateTime<Utc>,
    pub entries: ChangeRequestChangeFileEntries,
}

impl TryFrom<&ChangeFilesRequest> for ChangeFilesParam {
    type Error = ParameterError;

    fn try_from(request: &ChangeFilesRequest) -> Result<Self, Self::Error> {
        let partition_time = to_partition_time(request.partition_time)?;
        let idempotency_key = to_idempotency_key(request.idempotency_key.clone())?;

        let added_files = request
            .file_add_entries
            .iter()
            .map(|f| File::new(request.tenant_id, partition_time, f.path.clone(), f.size))
            .collect();
        let entries = ChangeRequestChangeFileEntries::new(added_files);

        let param = Self {
            idempotency_key,
            tenant_id: request.tenant_id,
            partition_time,
            entries,
        };
        Ok(param)
    }
}
