use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use test_utils::{init_logging, temp_dir};
use yorick::{
    BufferedIoConfig,
    DefaultCompactionPolicy,
    NoCompactionPolicy,
    StorageBackend,
    StorageServiceConfig,
    YorickStorageService,
};

#[tokio::test]
async fn test_compaction_buffered_io() -> Result<()> {
    init_logging();

    test_storage_impl(|| async {
        let buffered = BufferedIoConfig::default_for_test();
        StorageBackend::create_buffered_io(buffered)
            .await
            .expect("Create buffered IO backend")
    })
    .await
}

#[cfg(feature = "direct-io-backend")]
#[tokio::test]
async fn test_compaction_direct_io() -> Result<()> {
    use yorick::DirectIoConfig;

    init_logging();

    test_storage_impl(|| async {
        let buffered = DirectIoConfig::default_for_test();
        StorageBackend::create_direct_io(buffered)
            .await
            .expect("Create buffered IO backend")
    })
    .await
}

async fn test_storage_impl<BF, F>(backend_factory: BF) -> Result<()>
where
    BF: Fn() -> F,
    F: Future<Output = StorageBackend>,
{
    let dir = temp_dir();
    let res = test_storage_inner(dir.clone(), backend_factory).await;
    std::fs::remove_dir_all(dir).unwrap();
    res?;

    Ok(())
}

async fn test_storage_inner<BF, F>(dir: PathBuf, backend_factory: BF) -> Result<()>
where
    BF: Fn() -> F,
    F: Future<Output = StorageBackend>,
{
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 512 << 10,
    };

    let service = YorickStorageService::create_with_compaction(
        backend_factory().await,
        config.clone(),
        NoCompactionPolicy,
    )
    .await
    .expect("Failed to create service");

    let mut ctx = service.create_write_ctx();
    ctx.write_blob(1, 0, vec![0u8; 270 << 10]).await.unwrap();
    ctx.write_blob(2, 0, vec![0u8; 270 << 10]).await.unwrap();
    ctx.commit().await.unwrap();

    // Give the background task a chance to swap the writers.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut ctx = service.create_write_ctx();
    ctx.write_blob(3, 0, vec![0u8; 128 << 10]).await.unwrap();
    ctx.write_blob(4, 0, vec![0u8; 128 << 10]).await.unwrap();
    ctx.commit().await.unwrap();

    // Close the existing service.
    service.shutdown().unwrap();

    // Let services cleanup
    tokio::time::sleep(Duration::from_secs(2)).await;

    let file_count = count_data_files(&config.data_path())?;
    assert_eq!(file_count, 2, "File counts should match");

    // Create a new service with a higher max file size so files can be merged.
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 512 << 20,
    };

    let service = YorickStorageService::create_with_compaction(
        backend_factory().await,
        config.clone(),
        DefaultCompactionPolicy,
    )
    .await
    .expect("Failed to re-create service");

    // We create +1 writer on startup
    let file_count = count_data_files(&config.data_path())?;
    assert_eq!(file_count, 3, "File counts should match");

    // Manually trigger a compaction.
    service.start_compaction().await;

    // Give a chance for the compaction to run.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Our file count should go back down to 2 here because we create
    // +1 file on starting the service, and another +1 file when the resulting
    // files are compacted, so we have compacted two older writer files into 1.
    let file_count = count_data_files(&config.data_path())?;
    assert_eq!(file_count, 2, "File counts should match");

    service.shutdown().unwrap();

    Ok(())
}

fn count_data_files(path: &Path) -> Result<usize> {
    let mut count = 0;

    for entry in path.read_dir()? {
        let _entry = entry?;
        count += 1;
    }

    Ok(count)
}
