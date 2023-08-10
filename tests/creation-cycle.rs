use std::time::Duration;

use test_utils::{init_logging, temp_dir};
use yorick::{BufferedIoConfig, StorageServiceConfig};

#[tokio::test]
async fn test_buffered_io_basic_creation() {
    init_logging();

    let buffered = BufferedIoConfig::default();
    let backend = yorick::StorageBackend::create_buffered_io(buffered)
        .await
        .expect("Create buffered IO backend");

    let dir = temp_dir();
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 512 << 10,
    };

    let service = yorick::YorickStorageService::create(backend, config)
        .await
        .expect("Create service");

    service.shutdown().expect("Shutdown");

    std::fs::remove_dir_all(dir).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[cfg(feature = "direct-io-backend")]
#[tokio::test]
async fn test_direct_io_basic_creation() {
    init_logging();

    let buffered = BufferedIoConfig::default();
    let backend = yorick::StorageBackend::create_buffered_io(buffered)
        .await
        .expect("Create buffered IO backend");

    let dir = temp_dir();
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 512 << 10,
    };

    let service = yorick::YorickStorageService::create(backend, config)
        .await
        .expect("Create service");

    service.shutdown().expect("Shutdown");

    std::fs::remove_dir_all(dir).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
}
