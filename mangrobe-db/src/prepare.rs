use crate::grpc::api_client::ApiClient;
use crate::grpc::proto::{
    FileAddEntry, FileCompactDstEntry, FileCompactSrcEntry, FileDeleteEntry, LockFile,
};
use crate::infrastructure::s3::store::create_rustfs;
use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::array::ArrayRef as ArrowArrayRef;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use uuid::Uuid;
use vortex::arrays::ChunkedArray;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::file::VortexWriteOptions;
use vortex_array::arrow::FromArrowArray;
use vortex_array::{ArrayRef, IntoArray};

pub async fn prepare() -> Result<(), anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    let files = create_vortex_files(&temp_dir).await?;
    let rustfs = create_rustfs()?;

    let mut added_files: Vec<FileAddEntry> = vec![];
    for filename in files.iter() {
        let location = &Path::parse(filename.clone())?;

        let data = fs::read(dir_path.join(filename).as_path())?;
        let size = data.len();
        let payload = PutPayload::from_bytes(data.into());
        rustfs.put(location, payload).await?;

        added_files.push(FileAddEntry {
            path: filename.into(),
            size: size as i64,
        })
    }

    let conn = tonic::transport::Endpoint::new("http://[::1]:50051")?
        .connect()
        .await?;
    let api_client = ApiClient::new(conn);
    let response = api_client.add_files(0, added_files).await?;

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
    VortexWriteOptions::default()
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
        FileAddEntry {
            path: "a.txt".into(),
            size: 1,
        },
        FileAddEntry {
            path: "b.txt".into(),
            size: 2,
        },
        FileAddEntry {
            path: "c.txt".into(),
            size: 3,
        },
        FileAddEntry {
            path: "d.txt".into(),
            size: 4,
        },
    ];
    let response = api_client.add_files(stream_id, file_add_entries).await?;
    println!("add_files! id={:?}", response.get_ref().commit_id);

    let lock_key = Uuid::now_v7();
    let lock_target = vec![
        LockFile {
            path: "a.txt".into(),
        },
        LockFile {
            path: "b.txt".into(),
        },
    ];
    let response = api_client
        .acquire_lock(lock_key, stream_id, lock_target)
        .await?;
    println!(
        "locked! locked_file_count={:?}",
        response.get_ref().files.len()
    );

    let src_file_entries = vec![
        FileCompactSrcEntry {
            path: "a.txt".into(),
        },
        FileCompactSrcEntry {
            path: "b.txt".into(),
        },
    ];
    let dst_file_entry = FileCompactDstEntry {
        path: "a_compact.txt".into(),
        size: 123,
    };
    let response = api_client
        .compact_files(lock_key, stream_id, src_file_entries, dst_file_entry)
        .await?;
    println!("compacted! id={:?}", response.get_ref().commit_id);

    let lock_key = Uuid::now_v7();
    let lock_target = vec![LockFile {
        path: "c.txt".into(),
    }];
    let response = api_client
        .acquire_lock(lock_key, stream_id, lock_target)
        .await?;
    println!(
        "locked! locked_file_count={:?}",
        response.get_ref().files.len()
    );
    let file_delete_entries = vec![FileDeleteEntry {
        path: "c.txt".into(),
    }];
    let response = api_client
        .change_files(lock_key, stream_id, file_delete_entries)
        .await?;
    println!("change_files! id={:?}", response.get_ref().commit_id);

    Ok(())
}
