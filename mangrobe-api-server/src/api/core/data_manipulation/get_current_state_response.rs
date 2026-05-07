use crate::api::grpc::proto::{CurrentStatePartition, File, GetCurrentStateResponse};
use crate::domain::model::file::FileWithId;
use crate::domain::model::snapshot::Snapshot;
use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use std::collections::BTreeMap;

pub(crate) fn build_get_current_state_response(snapshot: Snapshot) -> GetCurrentStateResponse {
    GetCurrentStateResponse {
        commit_id: snapshot.commit_id.map(|id| id.to_string()),
        partitions: build_current_state_partitions(&snapshot.files),
    }
}

fn build_current_state_partitions(files: &[FileWithId]) -> Vec<CurrentStatePartition> {
    let mut partitions: BTreeMap<DateTime<Utc>, Vec<File>> = BTreeMap::new();
    for file in files {
        partitions
            .entry(file.file.partition_time)
            .or_default()
            .push(File {
                file_id: file.id.val().to_string(),
                path: file.file.path.path(),
                size: file.file.size,
            });
    }

    partitions
        .into_iter()
        .map(|(partition_time, files)| CurrentStatePartition {
            partition_time: Some(to_timestamp(partition_time)),
            files,
        })
        .collect()
}

fn to_timestamp(datetime: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: datetime.timestamp(),
        nanos: datetime.timestamp_subsec_nanos() as i32,
    }
}
