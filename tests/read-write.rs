use std::time::Duration;

use test_utils::{init_logging, temp_dir};
use yorick::{BufferedIoConfig, StorageServiceConfig, YorickStorageService};

#[tokio::test]
async fn test_buffered_io_read_write() {
    init_logging();

    let buffered = BufferedIoConfig::default_for_test();
    let backend = yorick::StorageBackend::create_buffered_io(buffered)
        .await
        .expect("Create buffered IO backend");

    let dir = temp_dir();
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 512 << 10,
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
async fn test_direct_io_read_write() {
    init_logging();

    use yorick::DirectIoConfig;

    let direct = DirectIoConfig::default_for_test();
    let backend = yorick::StorageBackend::create_direct_io(direct)
        .await
        .expect("Create buffered IO backend");

    let dir = temp_dir();
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 512 << 10,
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
    ctx.write_blob(1, 0, b"Hello, world")
        .await
        .expect("Write data");
    ctx.commit().await.expect("Commit changes");

    let ctx = service.create_read_ctx();
    let result = ctx
        .read_blob(1)
        .await
        .expect("Read blob")
        .expect("Blob should exist");
    assert_eq!(
        result.data.as_ref(),
        b"Hello, world".as_slice(),
        "Blob data should match",
    );

    let mut ctx = service.create_write_ctx();
    ctx.write_blob(2, 4, b"").await.expect("Write data");
    ctx.commit().await.expect("Commit changes");

    let ctx = service.create_read_ctx();
    let result = ctx
        .read_blob(2)
        .await
        .expect("Read blob")
        .expect("Blob should exist");
    assert_eq!(result.data.as_ref(), &[], "Blob data should match",);

    service.shutdown().expect("Shutdown");
}
