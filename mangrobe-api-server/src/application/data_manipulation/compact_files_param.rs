use crate::application::data_manipulation::param_util::{to_idempotency_key, to_partition_time};
use crate::domain::model::change_request_compact_file_entry::{
    ChangeRequestCompactFileEntry, ChangeRequestFileCompactDstEntry, FilePath,
};
use crate::domain::model::file::File;
use crate::domain::model::idempotency_key::IdempotencyKey;
use crate::grpc::proto::CompactFilesRequest;
use crate::util::error::ParameterError;
use chrono::{DateTime, Utc};

pub struct CompactFilesParam {
    pub idempotency_key: IdempotencyKey,
    pub tenant_id: i64,
    pub partition_time: DateTime<Utc>,
    pub entry: ChangeRequestCompactFileEntry,
}

impl TryFrom<&CompactFilesRequest> for CompactFilesParam {
    type Error = ParameterError;

    fn try_from(request: &CompactFilesRequest) -> Result<Self, Self::Error> {
        let partition_time = to_partition_time(request.partition_time)?;
        let idempotency_key = to_idempotency_key(request.idempotency_key.clone())?;

        let src_files: Vec<_> = request
            .src_file_entries
            .iter()
            .map(|f| FilePath::new(f.path.clone()))
            .collect();
        if src_files.is_empty() {
            return Err(ParameterError::Required("src_file_entries".to_string()));
        }

        let Some(ref req_dst_file) = request.dst_file_entry else {
            return Err(ParameterError::Required("dst_file_entry".to_string()));
        };
        let dest_file =
            ChangeRequestFileCompactDstEntry::new(req_dst_file.path.clone(), req_dst_file.size);
        let entry = ChangeRequestCompactFileEntry::new(
            request.tenant_id,
            partition_time,
            src_files,
            dest_file,
        );

        let param = Self {
            idempotency_key,
            tenant_id: request.tenant_id,
            partition_time,
            entry,
        };
        Ok(param)
    }
}
