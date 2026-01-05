mod operation;

use crate::operation::{
    add_files, change_files, compact_files, lock, print_current_files, release_lock,
};
use mangrobe_lab::{ApiClient, Stream};
use std::env;

const DEFAULT_MANGROBE_API_ADDR: &str = "http://[::1]:50051";
const QUERY_TABLE_ID: i64 = 901;

#[tokio::main]
async fn main() {
    println!("Running api-demo...");

    let api_server_addr = env::var("MANGROBE_API_ADDR").unwrap_or(DEFAULT_MANGROBE_API_ADDR.into());
    run(api_server_addr).await.unwrap();
}

async fn run(api_server_addr: String) -> Result<(), anyhow::Error> {
    let conn = tonic::transport::Endpoint::new(api_server_addr)?
        .connect()
        .await?;
    let mut api_client = ApiClient::new(conn);

    let stream = Stream::new_with_random_stream_id(QUERY_TABLE_ID)?;

    println!("\nAdding files...");
    let files: Vec<&str> = vec!["file1.txt", "file2.txt", "file3.txt", "file4.txt"];
    add_files(&api_client, &stream, files).await?;
    print_current_files(&api_client, &stream).await?;

    println!("\nCompacting files...");
    let compact_src_files = vec!["file1.txt", "file2.txt"];
    let compact_dst_file = "compacted.txt";
    let lock_key = lock(&mut api_client, &stream, &compact_src_files).await?;
    compact_files(
        &mut api_client,
        &stream,
        lock_key,
        compact_src_files,
        compact_dst_file,
    )
    .await?;
    print_current_files(&api_client, &stream).await?;

    println!("\nDeleting files...");
    let delete_target_files = vec!["file3.txt".into()];
    let lock_key = lock(&mut api_client, &stream, &delete_target_files).await?;
    change_files(&mut api_client, &stream, lock_key, delete_target_files).await?;
    print_current_files(&api_client, &stream).await?;

    println!("\nLocking files with no modification...");
    let nop_target_files = vec!["file4.txt".into()];
    let lock_key = lock(&mut api_client, &stream, &nop_target_files).await?;
    release_lock(&mut api_client, lock_key).await?;
    print_current_files(&api_client, &stream).await?;

    Ok(())
}
