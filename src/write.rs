use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use exponential_backoff::Backoff;
use parking_lot::RwLock;
use tokio::time::interval;

use crate::{FileKey, FileWriter, StorageBackend};

pub struct WriteContext {}

/// A background actor that watches the
/// current active writer, and changes to a new writer
/// once one file has reached a threshold.
pub(crate) struct WriterSizeController {
    threshold: u64,
    next_file_key: u32,
    base_path: PathBuf,
    backend: StorageBackend,
    stop_signal: Arc<AtomicBool>,
    writer_context: WriterContext,
}

impl WriterSizeController {
    /// Spawns a new writer size controller actor, returning a stop signal.
    pub async fn spawn(
        threshold: u64,
        next_file_key: u32,
        base_path: PathBuf,
        backend: StorageBackend,
        writer_context: WriterContext,
    ) -> Arc<AtomicBool> {
        let stop_signal = Arc::new(AtomicBool::new(false));

        let actor = Self {
            threshold,
            next_file_key,
            base_path,
            backend,
            stop_signal: stop_signal.clone(),
            writer_context,
        };

        tokio::spawn(actor.run());

        stop_signal
    }

    #[instrument("writer-size-controller", skip_all)]
    async fn run(mut self) {
        let mut interval = interval(Duration::from_secs(1));
        loop {
            interval.tick().await;

            if self.stop_signal.load(Ordering::Relaxed) {
                break;
            }

            let size = self.writer_context.current_size();
            if size < self.threshold {
                continue;
            }

            info!(
                size = size,
                next_file_key = self.next_file_key,
                "Writer is full, preparing to create new writer",
            );

            let backoff =
                Backoff::new(4, Duration::from_millis(500), Duration::from_secs(5));
            let mut attempt = 0;
            for backoff in &backoff {
                attempt += 1;
                match self.create_new_writer().await {
                    Ok(new_file_key) => {
                        info!(
                            new_file_key = new_file_key,
                            next_file_key = self.next_file_key,
                            "New writer has been created",
                        );

                        break;
                    },
                    Err(e) => {
                        error!(
                            error = ?e,
                            attempt = attempt,
                            "Failed to create new writer, retrying in {backoff:?}",
                        );
                    },
                }
            }
        }
    }

    /// Creates a new writer and replaces the old (now full) writer.
    async fn create_new_writer(&mut self) -> io::Result<u32> {
        let new_file_key = self.next_file_key;
        self.next_file_key += 1;

        let path = crate::get_data_file(&self.base_path, FileKey(new_file_key));
        let writer = self
            .backend
            .open_writer(FileKey(new_file_key), &path)
            .await?;

        self.writer_context.set_writer(writer);

        Ok(new_file_key)
    }
}

#[derive(Clone)]
pub(crate) struct WriterContext {
    /// The currently active file writer.
    active_writer: Arc<RwLock<FileWriter>>,
}

impl WriterContext {
    pub(crate) fn new(writer: FileWriter) -> Self {
        Self {
            active_writer: Arc::new(RwLock::new(writer)),
        }
    }

    pub(crate) fn current_size(&self) -> u64 {
        self.active_writer.read().size()
    }

    pub(crate) fn set_writer(&self, writer: FileWriter) {
        (*self.active_writer.write()) = writer;
    }
}
