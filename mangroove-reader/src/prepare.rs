use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_array::StringArray;
use arrow_array::array::ArrayRef as ArrowArrayRef;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use vortex;
use vortex::Array;
use vortex::arrays::ChunkedArray;
use vortex::buffer::buffer;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::expr::{root, select};
use vortex::file::{VortexOpenOptions, VortexWriteOptions};
use vortex_array::arrow::FromArrowArray;
use vortex_array::stream::ArrayStreamExt;
use vortex_array::{ArrayRef, ArrayVisitor, IntoArray};

pub(crate) async fn prepare(target_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    println!("Preparing files...");

    let files = create_vortex_files(target_dir).await?;
    println!("Checking files...");
    read_vortex(target_dir, files).await?;

    println!("Preparation finished!");
    Ok(())
}

async fn create_vortex_files(
    target_dir: &PathBuf,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    create_vortex(target_dir, "example1.vortex", 1, 1000).await?;
    create_vortex(target_dir, "example2.vortex", 20000, 23100).await?;

    Ok(vec![
        "example1.vortex".to_string(),
        "example2.vortex".to_string(),
    ])
}

async fn create_vortex(
    target_dir: &PathBuf,
    filename: &str,
    start: i32,
    end: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut ids: Vec<i32> = vec![];
    let mut codes: Vec<i32> = vec![];
    let mut names: Vec<String> = vec![];

    for i in start..end {
        ids.push(i);
        codes.push(i * -1);
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
            &mut tokio::fs::File::create(target_dir.join(filename)).await?,
            vortex_array.to_array_stream(),
        )
        .await?;

    Ok(())
}

async fn read_vortex(
    target_dir: &PathBuf,
    filenames: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    const POS: u64 = 100;

    for filename in filenames {
        println!("value at {} (file={})", POS, filename);
        let f = VortexOpenOptions::new()
            .open(target_dir.join(filename))
            .await?;
        let array = f
            .scan()?
            .with_row_indices(buffer![POS])
            .with_projection(select(["id", "code"], root()))
            .into_array_stream()?
            .read_all()
            .await?
            .to_canonical()
            .into_array();

        for child in array.children() {
            for val in child.to_array_iterator() {
                let z = val?;
                println!("{:?}: {:?}", z.dtype(), z.as_primitive_typed().value(0));
            }
        }
    }

    Ok(())
}
