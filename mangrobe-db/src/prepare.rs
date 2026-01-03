use crate::grpc::api_client::ApiClient;
use crate::grpc::proto::{
    AcquireFileLockEntry, AcquireFileLockFileInfoEntry, AddFileEntry, AddFileInfoEntry,
    ChangeFileDeleteEntry, ChangeFileEntry, CompactFileDstEntry, CompactFileEntry,
    CompactFileInfoEntry, CompactFileSrcEntry,
};
use crate::infrastructure::s3::store::create_rustfs;
use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::array::ArrayRef as ArrowArrayRef;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use prost_types::Timestamp;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use uuid::Uuid;
use vortex::VortexSessionDefault;
use vortex::arrays::ChunkedArray;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::file::VortexWriteOptions;
use vortex::session::VortexSession;
use vortex_array::arrow::FromArrowArray;
use vortex_array::{ArrayRef, IntoArray};

const DEFAULT_PARTITION_TIME: Timestamp = Timestamp {
    seconds: 0,
    nanos: 0,
};

pub const TABLE_ID: i64 = 0;
pub const STREAM_ID: i64 = 0;

pub async fn prepare() -> Result<(), anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    let files = create_vortex_files(&temp_dir).await?;
    let rustfs = create_rustfs()?;

    let mut add_file_entry = AddFileEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        file_info_entries: vec![],
    };
    for filename in files.iter() {
        let location = &Path::parse(filename.clone())?;

        let data = fs::read(dir_path.join(filename).as_path())?;
        let size = data.len();
        let payload = PutPayload::from_bytes(data.into());
        rustfs.put(location, payload).await?;

        add_file_entry.file_info_entries.push(AddFileInfoEntry {
            path: filename.into(),
            size: size as i64,
        })
    }

    let conn = tonic::transport::Endpoint::new("http://[::1]:50051")?
        .connect()
        .await?;
    let api_client = ApiClient::new(conn);
    let response = api_client
        .add_files(TABLE_ID, STREAM_ID, vec![add_file_entry])
        .await?;

    println!("success! id={:?}", response.get_ref().commit_id);
    Ok(())
}

async fn create_vortex_files(dir: &TempDir) -> Result<Vec<String>, anyhow::Error> {
    create_vortex(dir.path().join("example1.vortex"), 1, 1000).await?;
    create_vortex(dir.path().join("example2.vortex"), 20000, 23100).await?;

    Ok(vec![
        "example1.vortex".to_string(),
        "example2.vortex".to_string(),
    ])
}

async fn create_vortex(filename: PathBuf, start: i32, end: i32) -> Result<(), anyhow::Error> {
    let mut ids: Vec<i32> = vec![];
    let mut codes: Vec<i32> = vec![];
    let mut names: Vec<String> = vec![];

    for i in start..end {
        ids.push(i);
        codes.push(-i);
        names.push(format!("hello {} world {}", i, i));
    }

    let ids = Int32Array::from(ids);
    let codes = Int32Array::from(codes);
    let names = StringArray::from(names);

    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrowArrayRef),
        ("code", Arc::new(codes) as ArrowArrayRef),
        ("name", Arc::new(names) as ArrowArrayRef),
    ])
    .unwrap();

    let dtype = DType::from_arrow(batch.schema());
    let chunks = vec![ArrayRef::from_arrow(batch, false)];
    let vortex_array = ChunkedArray::try_new(chunks, dtype)?.into_array();

    // Write a Vortex file with the default compression and layout strategy.
    VortexWriteOptions::new(VortexSession::default())
        .write(
            &mut tokio::fs::File::create(filename).await?,
            vortex_array.to_array_stream(),
        )
        .await?;
    Ok(())
}

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
