use std::env::temp_dir;
use std::path::PathBuf;
use std::time::Duration;

use uuid::Uuid;
use yorick::{BufferedIoConfig, StorageServiceConfig};

#[tokio::test]
async fn test_buffered_io_basic_creation() {
    let buffered = BufferedIoConfig::default();
    let backend = yorick::StorageBackend::create_blocking_io(buffered)
        .await
        .expect("Create buffered IO backend");

    let dir = tmp_dir();
    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 512 << 10,
    };

    let service = yorick::YorickStorageService::create(backend, config)
        .await
        .expect("Create service");

    service.shutdown();

    std::fs::remove_dir_all(dir).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
}

fn tmp_dir() -> PathBuf {
    let path = temp_dir().join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&path).unwrap();
    path
}
