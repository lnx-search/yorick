use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use std::{cmp, io};

use glommio::{LocalExecutorBuilder, Placement};
use tokio::sync::{oneshot, Semaphore};

pub(crate) use self::reader::ReaderMailbox;
use self::reader::ReaderTask;
pub(crate) use self::writer::WriterMailbox;
use self::writer::WriterTask;
use crate::FileKey;

mod reader;
mod writer;

/// The default number of concurrent reads that can happen
/// across the entire backend.
pub const DEFAULT_MAX_READ_CONCURRENCY: usize = 64;

/// The buffer type used when passing values to write.
pub type WriteBuffer = Box<dyn AsRef<[u8]>>;
type BoxedTask = Box<dyn Task>;
type TasksTx = flume::Sender<BoxedTask>;
type TasksRx = flume::Receiver<BoxedTask>;

#[async_trait::async_trait]
trait Task: Send + Sync + 'static {
    /// Called in the context of a given runtime which will manage
    /// the task.
    async fn spawn(self);
}

pub(super) fn lost_contact_error() -> io::Error {
    warn!("Mailbox lost contact with actor that should be running");
    io::Error::new(ErrorKind::Other, "Mailbox lost contact with actor")
}

#[derive(Debug, Copy, Clone)]
/// The configuration options for the direct IO backend.
pub struct DirectIoConfig {
    /// The maximum number of concurrent reads that can happen at once.
    ///
    /// The default is `64`, but you may need to change this number depending on
    /// hardware and access patterns.
    pub max_read_concurrency: usize,
    /// Amount of memory to reserve for storage I/O.
    ///
    /// This will be pre-allocated and registered with io_uring.
    /// It is still possible to use more than that, but it will come from the standard allocator
    /// and performance will suffer.
    ///
    /// The system will always try to allocate at least 64 kiB for I/O memory, and the default is 10 MiB.
    pub io_memory: usize,
    /// The number of threads to spawn as the executor pool.
    pub num_threads: usize,
}

impl Default for DirectIoConfig {
    fn default() -> Self {
        Self {
            max_read_concurrency: DEFAULT_MAX_READ_CONCURRENCY,
            io_memory: 10 << 20,
            num_threads: cmp::min(num_cpus::get(), 4),
        }
    }
}

#[derive(Clone)]
/// The primary backend for direct IO operations.
///
/// Internally this maintains an asynchronous pool for scheduling operations.
pub struct DirectIoBackend {
    /// The channel to submit new tasks to be scheduled on a thread.
    task_submitter: TasksTx,
    /// The global limiter preventing too many reads from attempting
    /// to occur at one time.
    global_read_limiter: Arc<Semaphore>,
}

impl DirectIoBackend {
    #[instrument("direct-io")]
    /// Creates a new direct IO backend with a given config.
    pub async fn create(config: DirectIoConfig) -> io::Result<Self> {
        info!("Creating new direct IO backend with config");

        let global_read_limiter = Arc::new(Semaphore::new(config.max_read_concurrency));
        let (tasks_tx, tasks_rx) = flume::bounded(config.num_threads * 2);

        for shard_id in 0..config.num_threads {
            spawn_executor_thread(shard_id, config, tasks_rx.clone()).await?;
        }

        Ok(Self {
            task_submitter: tasks_tx,
            global_read_limiter,
        })
    }

    #[instrument("open-writer", skip(self))]
    /// Opens a new writer using the backend.
    pub async fn open_writer(
        &self,
        file_key: FileKey,
        path: &Path,
    ) -> io::Result<WriterMailbox> {
        let (tx, rx) = oneshot::channel();
        let task = WriterTask {
            file_key,
            path: path.to_path_buf(),
            tx,
        };

        self.schedule_task(Box::new(task)).await?;

        rx.await.map_err(|_| lost_contact_error())?
    }

    #[instrument("open-reader", skip(self))]
    /// Opens a new reader using the backend.
    pub async fn open_reader(
        &self,
        file_key: FileKey,
        path: &Path,
    ) -> io::Result<ReaderMailbox> {
        let (tx, rx) = oneshot::channel();
        let task = ReaderTask {
            file_key,
            path: path.to_path_buf(),
            tx,
            global_limiter: self.global_read_limiter.clone(),
        };

        self.schedule_task(Box::new(task)).await?;

        rx.await.map_err(|_| lost_contact_error())?
    }

    async fn schedule_task(&self, op: BoxedTask) -> io::Result<()> {
        self.task_submitter
            .send_async(op)
            .await
            .map_err(|_| lost_contact_error())
    }
}

async fn spawn_executor_thread(
    shard_id: usize,
    config: DirectIoConfig,
    tasks_rx: TasksRx,
) -> io::Result<()> {
    let name = format!("yorick-executor-{shard_id}");

    let (waiter_tx, waiter_rx) = oneshot::channel();

    let handle = LocalExecutorBuilder::new(Placement::Unbound)
        .io_memory(config.io_memory)
        .name(&name)
        .spawn(move || executor_task(shard_id, tasks_rx, waiter_tx))
        .expect("Spawn local executor");

    if waiter_rx.await.is_err() {
        return match handle.join() {
            Err(e) => {
                error!(error = ?e, "Failed to spawn executor due to error");
                Err(io::Error::new(ErrorKind::Other, e.to_string()))
            },
            Ok(()) => {
                error!("Executor exited with a successful result, this is a bug!");
                Err(io::Error::new(ErrorKind::Other, "Invalid executor state"))
            },
        };
    }

    debug!("Executor shard spawned successfully");

    Ok(())
}

#[instrument("direct-io", skip(tasks_rx, waiter_tx))]
/// The main task entrypoint.
async fn executor_task(
    shard_id: usize,
    tasks_rx: TasksRx,
    waiter_tx: oneshot::Sender<()>,
) {
    let _ = waiter_tx.send(());

    while let Ok(task) = tasks_rx.recv_async().await {
        task.spawn().await;
    }

    // Not needed, just for logging and tracing.
    drop(shard_id);
}
