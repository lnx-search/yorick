mod reader;
mod writer;

use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

pub(crate) use reader::Reader;
use tokio::io;
use tokio::sync::oneshot;
pub(crate) use writer::Writer;

use crate::FileKey;

#[derive(Debug, Copy, Clone)]
/// The configuration options for the buffered IO backend.
pub struct BufferedIoConfig {
    /// The number of threads used for the read executor.
    ///
    /// By default this is `num_cpu_cores`.
    pub io_threads: usize,
}

impl Default for BufferedIoConfig {
    fn default() -> Self {
        Self {
            io_threads: num_cpus::get(),
        }
    }
}

#[derive(Clone)]
/// The primary backend for buffered IO operations.
///
/// Internally this maintains an threadpool pool for scheduling operations.
pub struct BufferedIoBackend {
    pool: Arc<rayon::ThreadPool>,
}

impl BufferedIoBackend {
    #[instrument("buffered-io")]
    /// Creates a new buffered IO backend with a given config.
    pub fn create(config: BufferedIoConfig) -> io::Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.io_threads)
            .thread_name(|n| format!("yorick-executor-{n}"))
            .build()
            .map_err(|e| {
                error!(error = ?e, "Failed to build buffered IO threadpool");
                io::Error::new(ErrorKind::Other, e)
            })?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    #[instrument("open-writer", skip(self))]
    /// Opens a new writer using the backend.
    pub async fn open_writer(
        &self,
        file_key: FileKey,
        path: &Path,
    ) -> io::Result<writer::Writer> {
        writer::Writer::open_or_create(file_key, &path).await
    }

    #[instrument("open-reader", skip(self))]
    /// Opens a new reader using the backend.
    pub async fn open_reader(
        &self,
        file_key: FileKey,
        path: &Path,
    ) -> io::Result<reader::Reader> {
        let (tx, rx) = oneshot::channel();

        let path = path.to_path_buf();
        let pool = self.pool.clone();
        self.pool.spawn(move || {
            let reader = reader::Reader::open(file_key, &path, pool);
            let _ = tx.send(reader);
        });

        rx.await.expect("Threadpool should never die")
    }
}
