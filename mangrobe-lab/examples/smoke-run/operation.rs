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

pub async fn add_files(api_client: &ApiClient, stream: &Stream) -> Result<(), anyhow::Error> {
    let file_add_entries = vec![AddFileEntry {
        partition_time: Some(Timestamp {
            seconds: 0,
            nanos: 0,
        }),
        file_info_entries: vec![
            AddFileInfoEntry {
                path: "file1.txt".into(),
                size: 1,
            },
            AddFileInfoEntry {
                path: "file2.txt".into(),
                size: 2,
            },
            AddFileInfoEntry {
                path: "file3.txt".into(),
                size: 3,
            },
            AddFileInfoEntry {
                path: "file4.txt".into(),
                size: 4,
            },
        ],
    }];
    let response = api_client
        .add_files(stream.table_id, stream.stream_id, file_add_entries)
        .await?;

    println!("Run AddFiles! commit_id={:?}", response.get_ref().commit_id);

    Ok(())
}

pub async fn compact_files(
    api_client: &mut ApiClient,
    stream: &Stream,
    lock_key: Uuid,
    src_files: Vec<String>,
) -> Result<(), anyhow::Error> {
    let compact_file_entries = vec![CompactFileEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        file_info_entries: vec![CompactFileInfoEntry {
            src_entries: src_files
                .into_iter()
                .map(|f| CompactFileSrcEntry { path: f })
                .collect(),
            dst_entry: Some(CompactFileDstEntry {
                path: "compacted.txt".into(),
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
        "Run CompactFiles! commit_id={:?}",
        response.get_ref().commit_id
    );

    Ok(())
}

pub async fn change_files(
    api_client: &mut ApiClient,
    stream: &Stream,
    lock_key: Uuid,
    delete_files: Vec<String>,
) -> Result<(), anyhow::Error> {
    let change_file_entries = vec![ChangeFileEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        delete_entries: delete_files
            .into_iter()
            .map(|f| ChangeFileDeleteEntry { path: f })
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
        "Run ChangeFiles! commit_id={:?}",
        response.get_ref().commit_id
    );

    Ok(())
}

pub async fn lock(
    api_client: &mut ApiClient,
    stream: &Stream,
    target_files: &[String],
) -> Result<Uuid, anyhow::Error> {
    let lock_key = Uuid::now_v7();

    let acquire_file_lock_entries = vec![AcquireFileLockEntry {
        partition_time: Some(DEFAULT_PARTITION_TIME),
        acquire_file_info_entries: target_files
            .iter()
            .map(|file| AcquireFileLockFileInfoEntry { path: file.clone() })
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
