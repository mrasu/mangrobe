use arrow_array::array::ArrayRef as ArrowArrayRef;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use mangrobe_lab::proto::{AddFileEntry, AddFileInfoEntry};
use mangrobe_lab::{ApiClient, Stream, create_bucket_if_not_exists, create_rustfs};
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use prost_types::Timestamp;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::TempDir;
use tonic::transport::Endpoint;
use vortex::VortexSessionDefault;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::file::VortexWriteOptions;
use vortex::session::VortexSession;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrow::FromArrowArray;
use vortex_array::{ArrayRef, IntoArray};

const QUERY_TABLE_ID: i64 = 900;
const QUERY_PARTITION_TIME: Timestamp = Timestamp {
    seconds: 0,
    nanos: 0,
};

pub async fn prepare_table(bucket_name: String) -> Result<Stream, anyhow::Error> {
    let stream = Stream {
        table_id: QUERY_TABLE_ID,
        stream_id: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs() as i64,
    };

    create_bucket_if_not_exists(bucket_name).await?;

    Ok(stream)
}

pub async fn register_files(
    api_server_addr: String,
    stream: &Stream,
    bucket_name: String,
) -> Result<(), anyhow::Error> {
    let s3_cli = create_rustfs(bucket_name)?;

    let temp_dir = tempfile::tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    let files = create_vortex_files(&temp_dir).await?;

    let mut add_file_entry = AddFileEntry {
        partition_time: Some(QUERY_PARTITION_TIME),
        file_info_entries: vec![],
    };
    for filename in files.iter() {
        let location = &Path::parse(filename.clone())?;

        let data = fs::read(dir_path.join(filename).as_path())?;
        let size = data.len();
        let payload = PutPayload::from_bytes(data.into());
        s3_cli.put(location, payload).await?;

        add_file_entry.file_info_entries.push(AddFileInfoEntry {
            path: filename.into(),
            size: size as i64,
        })
    }

    let conn = Endpoint::new(api_server_addr)?.connect().await?;
    let api_client = ApiClient::new(conn);
    api_client
        .add_files(stream.table_id, stream.stream_id, vec![add_file_entry])
        .await?;

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
