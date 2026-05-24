use crate::api::core::util::param::partition::to_proto_partition_value;
use crate::api::grpc::proto::{CurrentStatePartition, File, GetCurrentStateResponse};
use crate::domain::model::file::FileWithId;
use crate::domain::model::partition::Partition;
use crate::domain::model::snapshot::Snapshot;
use crate::domain::model::table_definition::PartitionDataType;
use std::collections::BTreeMap;

pub(crate) fn build_get_current_state_response(snapshot: Snapshot) -> GetCurrentStateResponse {
    GetCurrentStateResponse {
        commit_id: snapshot.commit_id.map(|id| id.to_string()),
        partitions: build_current_state_partitions(&snapshot.partition_data_type, &snapshot.files),
    }
}

fn build_current_state_partitions(
    partition_data_type: &PartitionDataType,
    files: &[FileWithId],
) -> Vec<CurrentStatePartition> {
    let mut partitions: BTreeMap<Partition, Vec<File>> = BTreeMap::new();
    for file in files {
        partitions
            .entry(file.file.partition.clone())
            .or_default()
            .push(File {
                file_id: file.id.val().to_string(),
                path: file.file.path.path(),
                size: file.file.size,
            });
    }

    partitions
        .into_iter()
        .map(|(partition, files)| CurrentStatePartition {
            partition: Some(to_proto_partition_value(partition_data_type, partition)),
            files,
        })
        .collect()
}
