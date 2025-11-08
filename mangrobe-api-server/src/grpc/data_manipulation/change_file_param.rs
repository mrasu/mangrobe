use crate::domain::model::change_request_change_file_entries::{
    ChangeRequestChangeFileEntries, ChangeRequestFileAddEntry,
};
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::grpc::proto::ChangeFilesRequest;
use crate::grpc::util::error::{build_argument_required, build_invalid_argument};
use chrono::{DateTime, Utc};
use tonic::Status;

pub struct ChangeFileParam {
    pub idempotency_key: IdempotencyKey,
    pub tenant_id: i64,
    pub partition_time: DateTime<Utc>,
    pub entries: ChangeRequestChangeFileEntries,
}

impl TryFrom<&ChangeFilesRequest> for ChangeFileParam {
    type Error = Status;

    fn try_from(request: &ChangeFilesRequest) -> Result<Self, Self::Error> {
        let added_files = request
            .file_add_entries
            .iter()
            .map(|f| ChangeRequestFileAddEntry::new(f.path.clone(), f.size))
            .collect();
        let entries = ChangeRequestChangeFileEntries::new(added_files);

        let req_partition_time = request
            .partition_time
            .ok_or(build_argument_required("partition_time"))?;
        let partition_time =
            DateTime::from_timestamp(req_partition_time.seconds, req_partition_time.nanos as u32)
                .ok_or(build_invalid_argument(
                "partition_time",
                "out-of-range number of seconds or nanos",
            ))?;

        let idempotency_key = IdempotencyKey::try_from(request.idempotency_key.clone())
            .map_err(|msg| build_invalid_argument("idempotency_key", msg.as_str()))?;

        let param = Self {
            idempotency_key,
            tenant_id: request.tenant_id,
            partition_time,
            entries,
        };
        Ok(param)
    }
}
