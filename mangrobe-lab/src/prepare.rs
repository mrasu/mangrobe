use crate::grpc::api_client::ApiClient;
use crate::grpc::proto::{
    AcquireFileLockEntry, AcquireFileLockFileInfoEntry, AddFileEntry, AddFileInfoEntry,
    ChangeFileDeleteEntry, ChangeFileEntry, CompactFileDstEntry, CompactFileEntry,
    CompactFileInfoEntry, CompactFileSrcEntry,
};
use prost_types::Timestamp;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const DEFAULT_PARTITION_TIME: Timestamp = Timestamp {
    seconds: 0,
    nanos: 0,
};
const TABLE_ID: i64 = 0;

pub async fn smoke_run() -> Result<(), anyhow::Error> {
    let conn = tonic::transport::Endpoint::new("http://[::1]:50051")?
        .connect()
        .await?;

    let stream_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    println!("stream_id={}", stream_id);

    let mut api_client = ApiClient::new(conn);

    let file_add_entries = vec![
        AddFileEntry {
            partition_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            file_info_entries: vec![
                AddFileInfoEntry {
                    path: "a.txt".into(),
                    size: 1,
                },
                AddFileInfoEntry {
                    path: "b.txt".into(),
                    size: 2,
                },
                AddFileInfoEntry {
                    path: "c.txt".into(),
                    size: 3,
                },
            ],
        },
        AddFileEntry {
            partition_time: Some(Timestamp {
                seconds: 1000,
                nanos: 0,
            }),
            file_info_entries: vec![AddFileInfoEntry {
                path: "d.txt".into(),
                size: 4,
            }],
        },
    ];
    let response = api_client
        .add_files(TABLE_ID, stream_id, file_add_entries)
        .await?;
    println!("add_files! id={:?}", response.get_ref().commit_id);

    let lock_key = Uuid::now_v7();
    let acquire_file_lock_entries = vec![AcquireFileLockEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        acquire_file_info_entries: vec![
            AcquireFileLockFileInfoEntry {
                path: "a.txt".into(),
            },
            AcquireFileLockFileInfoEntry {
                path: "b.txt".into(),
            },
        ],
    }];
    let response = api_client
        .acquire_lock(lock_key, TABLE_ID, stream_id, acquire_file_lock_entries)
        .await?;
    println!(
        "locked! locked_file_count={:?}",
        response.get_ref().files.len()
    );

    let compact_file_entries = vec![CompactFileEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        file_info_entries: vec![CompactFileInfoEntry {
            src_entries: vec![
                CompactFileSrcEntry {
                    path: "a.txt".into(),
                },
                CompactFileSrcEntry {
                    path: "b.txt".into(),
                },
            ],
            dst_entry: Some(CompactFileDstEntry {
                path: "a_compact.txt".into(),
                size: 123,
            }),
        }],
    }];
    let response = api_client
        .compact_files(lock_key, TABLE_ID, stream_id, compact_file_entries)
        .await?;
    println!("compacted! id={:?}", response.get_ref().commit_id);

    let lock_key = Uuid::now_v7();
    let acquire_file_lock_entries = vec![AcquireFileLockEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        acquire_file_info_entries: vec![AcquireFileLockFileInfoEntry {
            path: "c.txt".into(),
        }],
    }];
    let response = api_client
        .acquire_lock(lock_key, TABLE_ID, stream_id, acquire_file_lock_entries)
        .await?;
    println!(
        "locked! locked_file_count={:?}",
        response.get_ref().files.len()
    );
    let change_file_entries = vec![ChangeFileEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        delete_entries: vec![ChangeFileDeleteEntry {
            path: "c.txt".into(),
        }],
    }];
    let response = api_client
        .change_files(lock_key, TABLE_ID, stream_id, change_file_entries)
        .await?;
    println!("change_files! id={:?}", response.get_ref().commit_id);

    let acquire_file_lock_entries = vec![AcquireFileLockEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        acquire_file_info_entries: vec![AcquireFileLockFileInfoEntry {
            path: "a_compact.txt".into(),
        }],
    }];

    let lock_key = Uuid::now_v7();
    let response = api_client
        .acquire_lock(lock_key, TABLE_ID, stream_id, acquire_file_lock_entries)
        .await?;
    println!("locked acquired! id={:?}", lock_key);
    let response = api_client.release_lock(lock_key).await?;
    println!("lock released! id={:?}", lock_key);

    Ok(())
}
