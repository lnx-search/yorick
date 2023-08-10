use std::time::Duration;

use test_utils::{init_logging, temp_dir};
use yorick::{BufferedIoConfig, StorageServiceConfig, YorickStorageService};

#[tokio::test]
async fn test_buffered_io_writer_change() {
    init_logging();

    let buffered = BufferedIoConfig::default();
    let backend = yorick::StorageBackend::create_buffered_io(buffered)
        .await
        .expect("Create buffered IO backend");

    let dir = temp_dir();
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 128,
    };

    let service = YorickStorageService::create(backend, config)
        .await
        .expect("Create service");

    test_storage(service).await;

    std::fs::remove_dir_all(dir).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[cfg(feature = "direct-io-backend")]
#[tokio::test]
async fn test_direct_io_writer_change() {
    init_logging();

    use yorick::DirectIoConfig;

    let direct = DirectIoConfig::default();
    let backend = yorick::StorageBackend::create_direct_io(direct)
        .await
        .expect("Create buffered IO backend");

    let dir = temp_dir();
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 128,
    };

    let service = YorickStorageService::create(backend, config)
        .await
        .expect("Create service");

    test_storage(service).await;

    std::fs::remove_dir_all(dir).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
}

async fn test_storage(service: YorickStorageService) {
    let mut ctx = service.create_write_ctx();
    // Write a big blob that puts us above the threshold.
    let write_id_1 = ctx.write_blob(1, 0, [0; 150]).await.expect("Write data");
    ctx.commit().await.expect("Commit changes");

    // Let the system check the file.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut ctx = service.create_write_ctx();
    let write_id_2 = ctx.write_blob(2, 4, b"").await.expect("Write data");
    ctx.commit().await.expect("Commit changes");

    assert_ne!(
        write_id_1.file_key(),
        write_id_2.file_key(),
        "A new writer should have been made"
    );

    let ctx = service.create_read_ctx();
    let result = ctx
        .read_blob(1)
        .await
        .expect("Read blob")
        .expect("Blob should exist");
    assert_eq!(result.info.file_key(), write_id_1.file_key());
    let result = ctx
        .read_blob(2)
        .await
        .expect("Read blob")
        .expect("Blob should exist");
    assert_eq!(result.info.file_key(), write_id_2.file_key());

    service.shutdown().expect("Shutdown");
}
