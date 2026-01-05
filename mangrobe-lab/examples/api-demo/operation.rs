use mangrobe_lab::proto::{
    AcquireFileLockEntry, AcquireFileLockFileInfoEntry, AddFileEntry, AddFileInfoEntry,
    ChangeFileDeleteEntry, ChangeFileEntry, CompactFileDstEntry, CompactFileEntry,
    CompactFileInfoEntry, CompactFileSrcEntry,
};
use mangrobe_lab::{ApiClient, Stream};
use prost_types::Timestamp;
use uuid::Uuid;

const DEFAULT_PARTITION_TIME: Timestamp = Timestamp {
    seconds: 0,
    nanos: 0,
};

pub async fn print_current_files(
    api_client: &ApiClient,
    stream: &Stream,
) -> Result<(), anyhow::Error> {
    let current_state = api_client
        .fetch_current_state(stream.table_id, stream.stream_id)
        .await?;

    let mut files = current_state
        .get_ref()
        .files
        .iter()
        .map(|f| f.path.clone())
        .collect::<Vec<_>>();
    files.sort();
    let files_text = files.join(", ");

    println!("Current files: {}", files_text);
    Ok(())
}

pub async fn add_files(
    api_client: &ApiClient,
    stream: &Stream,
    files: Vec<&str>,
) -> Result<(), anyhow::Error> {
    let file_add_entries = vec![AddFileEntry {
        partition_time: Some(Timestamp {
            seconds: 0,
            nanos: 0,
        }),
        file_info_entries: files
            .iter()
            .map(|path| AddFileInfoEntry {
                path: path.to_string(),
                size: 1,
            })
            .collect(),
    }];
    let response = api_client
        .add_files(stream.table_id, stream.stream_id, file_add_entries)
        .await?;

    println!(
        "Run AddFiles! files=[{}] (commit_id={})",
        files.join(", "),
        response.get_ref().commit_id
    );

    Ok(())
}

pub async fn compact_files(
    api_client: &mut ApiClient,
    stream: &Stream,
    lock_key: Uuid,
    src_files: Vec<&str>,
    dst_file: &str,
) -> Result<(), anyhow::Error> {
    let compact_file_entries = vec![CompactFileEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        file_info_entries: vec![CompactFileInfoEntry {
            src_entries: src_files
                .iter()
                .map(|f| CompactFileSrcEntry {
                    path: f.to_string(),
                })
                .collect(),
            dst_entry: Some(CompactFileDstEntry {
                path: dst_file.to_string(),
                size: 123,
            }),
        }],
    }];

    let response = api_client
        .compact_files(
            lock_key,
            stream.table_id,
            stream.stream_id,
            compact_file_entries,
        )
        .await?;

    println!(
        "Run CompactFiles! src=[{}], dst={} (commit_id={})",
        src_files.join(", "),
        dst_file,
        response.get_ref().commit_id
    );

    Ok(())
}

pub async fn change_files(
    api_client: &mut ApiClient,
    stream: &Stream,
    lock_key: Uuid,
    delete_files: Vec<&str>,
) -> Result<(), anyhow::Error> {
    let change_file_entries = vec![ChangeFileEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        delete_entries: delete_files
            .iter()
            .map(|f| ChangeFileDeleteEntry {
                path: f.to_string(),
            })
            .collect(),
    }];
    let response = api_client
        .change_files(
            lock_key,
            stream.table_id,
            stream.stream_id,
            change_file_entries,
        )
        .await?;

    println!(
        "Run ChangeFiles! delete=[{}], commit_id={}",
        delete_files.join(", "),
        response.get_ref().commit_id
    );

    Ok(())
}

pub async fn lock(
    api_client: &mut ApiClient,
    stream: &Stream,
    target_files: &[&str],
) -> Result<Uuid, anyhow::Error> {
    let lock_key = Uuid::now_v7();

    let acquire_file_lock_entries = vec![AcquireFileLockEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        acquire_file_info_entries: target_files
            .iter()
            .map(|file| AcquireFileLockFileInfoEntry {
                path: file.to_string(),
            })
            .collect(),
    }];
    let response = api_client
        .acquire_lock(
            lock_key,
            stream.table_id,
            stream.stream_id,
            acquire_file_lock_entries,
        )
        .await?;

    println!(
        "Run AcquireFileLock! key={}, locked_file_count={:?}",
        lock_key,
        response.get_ref().files.len()
    );

    Ok(lock_key)
}

pub async fn release_lock(
    api_client: &mut ApiClient,
    lock_key: Uuid,
) -> Result<Uuid, anyhow::Error> {
    api_client.release_lock(lock_key).await?;
    println!("Run ReleaseFileLock! key={}", lock_key,);

    Ok(lock_key)
}
