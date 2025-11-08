use crate::grpc::api_client::ApiClient;
use crate::grpc::proto::FileAddEntry;
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
use tempfile::TempDir;
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
    let response = api_client.add_files(added_files).await?;

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
