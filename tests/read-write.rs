use std::env::temp_dir;
use std::path::PathBuf;
use std::time::Duration;

use uuid::Uuid;
use yorick::{BufferedIoConfig, StorageServiceConfig, YorickStorageService};

#[tokio::test]
async fn test_buffered_io_read_write() {
    let buffered = BufferedIoConfig::default();
    let backend = yorick::StorageBackend::create_blocking_io(buffered)
        .await
        .expect("Create buffered IO backend");

    let dir = tmp_dir();
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 512 << 10,
    };

    let service = YorickStorageService::create(backend, config)
        .await
        .expect("Create service");

    test_storage(service).await;
}

#[cfg(feature = "direct-io-backend")]
#[tokio::test]
async fn test_direct_io_read_write() {
    use yorick::DirectIoConfig;

    let direct = DirectIoConfig::default();
    let backend = yorick::StorageBackend::create_direct_io(direct)
        .await
        .expect("Create buffered IO backend");

    let dir = tmp_dir();
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 512 << 10,
    };

    let service = YorickStorageService::create(backend, config)
        .await
        .expect("Create service");

    test_storage(service).await;
}

async fn test_storage(service: YorickStorageService) {
    let mut ctx = service.create_write_ctx();
    ctx.write_blob(1, 0, b"Hello, world")
        .await
        .expect("Write data");
    ctx.commit().await.expect("Commit changes");

    let ctx = service.create_read_ctx();
    let data = ctx.read_blob(1).await.expect("Read blob");
    assert_eq!(
        data.as_ref().map(|v| v.as_ref()),
        Some(b"Hello, world".as_slice()),
        "Blob data should match",
    );

    let mut ctx = service.create_write_ctx();
    ctx.write_blob(2, 4, b"").await.expect("Write data");
    ctx.commit().await.expect("Commit changes");

    let ctx = service.create_read_ctx();
    let data = ctx.read_blob(1).await.expect("Read blob");
    assert_eq!(
        data.as_ref().map(|v| v.as_ref()),
        Some(b"Hello, world".as_slice()),
        "Blob data should match",
    );

    service.shutdown();

    std::fs::remove_dir_all(dir).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
}

fn tmp_dir() -> PathBuf {
    let path = temp_dir().join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&path).unwrap();
    path
}
