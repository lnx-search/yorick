use std::mem;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use mimalloc::MiMalloc;
use tokio::task::JoinSet;
use uuid::Uuid;
use yorick::{
    BufferedIoConfig,
    DirectIoConfig,
    StorageBackend,
    StorageServiceConfig,
    YorickStorageService,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    tracing_subscriber::fmt::init();

    let buffered_config = BufferedIoConfig::default();
    let buffered_backend = StorageBackend::create_buffered_io(buffered_config).await?;
    test_write_perf(buffered_backend, 25).await?;

    let direct_config = DirectIoConfig::default();
    let direct_backend = StorageBackend::create_direct_io(direct_config).await?;
    test_write_perf(direct_backend, 25).await?;

    Ok(())
}

async fn test_write_perf(backend: StorageBackend, concurrency: usize) -> Result<()> {
    let dir = Path::new("../test-data").join(Uuid::new_v4().to_string());

    tokio::fs::create_dir_all(&dir).await?;

    let config = StorageServiceConfig {
        base_path: dir.clone(),
        max_file_size: 3 << 30,
    };

    let res = perf(config, backend, concurrency).await;

    let _ = tokio::fs::remove_dir_all(&dir).await;

    res
}

async fn perf(
    config: StorageServiceConfig,
    backend: StorageBackend,
    concurrency: usize,
) -> Result<()> {
    let service = YorickStorageService::create(backend, config).await?;

    let start = Instant::now();

    let num_bytes_written = Arc::new(AtomicUsize::new(0));
    let mut tasks = JoinSet::new();
    for c in 0..concurrency {
        let service = service.clone();
        let num_bytes_written = num_bytes_written.clone();

        tasks.spawn(async move {
            let mut ctx = service.create_write_ctx();
            let buffer = vec![0u8; 512 << 10];

            for i in 0u64..2500u64 {
                if i % 100 == 0 {
                    let tmp = mem::replace(&mut ctx, service.create_write_ctx());
                    tmp.commit().await?;
                }

                ctx.write_blob(i * c as u64, 0, buffer.clone()).await?;
                num_bytes_written.fetch_add(buffer.len(), Ordering::Acquire);
            }

            ctx.commit().await?;

            Ok::<_, anyhow::Error>(())
        });
    }

    while let Some(res) = tasks.join_next().await {
        res??;
    }

    println!(
        ">>> Completed test in {:?} writing {}",
        start.elapsed(),
        humansize::format_size(
            num_bytes_written.load(Ordering::Relaxed),
            humansize::DECIMAL
        ),
    );

    service.shutdown()?;

    Ok(())
}
