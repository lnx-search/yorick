use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use glommio::io::DmaFile;
use tokio::sync::{oneshot, Semaphore};

use crate::backends::directio::{lost_contact_error, Task};
use crate::backends::ReadBuffer;
use crate::FileKey;

type ReadOpTx = tachyonix::Sender<IoOp>;
type ReadOpRx = tachyonix::Receiver<IoOp>;

pub struct ReaderTask {
    pub(super) file_key: FileKey,
    pub(super) path: PathBuf,
    pub(super) tx: oneshot::Sender<io::Result<ReaderMailbox>>,
    pub(super) global_limiter: Arc<Semaphore>,
}

#[async_trait::async_trait]
impl Task for ReaderTask {
    async fn spawn(self) {
        let (tx, rx) = tachyonix::channel(5);

        let res = ReaderActor::open(
            self.file_key,
            &self.path,
            rx,
            self.global_limiter.clone(),
        )
        .await;

        let actor = match res {
            Ok(actor) => actor,
            Err(e) => {
                let _ = self.tx.send(Err(e));
                return;
            },
        };

        glommio::spawn_local(actor.run()).detach();

        let mailbox = ReaderMailbox { tx };
        let _ = self.tx.send(Ok(mailbox));
    }
}

#[derive(Clone)]
/// A mailbox for sending operations to the given writer.
pub struct ReaderMailbox {
    tx: ReadOpTx,
}

impl ReaderMailbox {
    /// Closes the currently open file.
    pub async fn close(&self) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send_op(IoOp::Close { tx }).await?;
        rx.await.map_err(|e| lost_contact_error())?
    }

    /// Performs a random read on the file at the given position reading `len` bytes.
    pub async fn read_at(&self, pos: usize, len: usize) -> io::Result<ReadBuffer> {
        let (tx, rx) = oneshot::channel();
        let ctx = ReadContext { pos, len, tx };

        self.send_op(IoOp::Read { ctx }).await?;
        rx.await.map_err(|e| lost_contact_error())?
    }

    async fn send_op(&self, op: IoOp) -> io::Result<()> {
        self.tx.send(op).await.map_err(|_| lost_contact_error())
    }
}

/// A reader that runs within the glommio runtime.
pub struct ReaderActor {
    file_key: FileKey,
    file: Rc<DmaFile>,
    ops: ReadOpRx,
    global_limiter: Arc<Semaphore>,
}

impl ReaderActor {
    #[instrument("open-reader")]
    /// Opens a new file for reading.
    async fn open(
        file_key: FileKey,
        path: &Path,
        ops: ReadOpRx,
        global_limiter: Arc<Semaphore>,
    ) -> io::Result<Self> {
        let file = DmaFile::open(path).await?;
        let file = Rc::new(file);
        Ok(Self {
            file_key,
            file,
            ops,
            global_limiter,
        })
    }

    #[instrument("reader", skip_all, fields(file_key = ?self.file_key))]
    /// Runs the actor, listening for read events.
    async fn run(mut self) {
        while let Ok(op) = self.ops.recv().await {
            match op {
                IoOp::Read { ctx } => {
                    self.spawn_reader(ctx);
                },
                IoOp::Close { tx } => {
                    let res = self.file.close_rc().await;

                    if let Err(e) = res {
                        error!(error = ?e, "Failed to close reader");
                        let _ = tx.send(Err(e.into()));
                    } else {
                        let _ = tx.send(Ok(()));
                    }
                    return;
                },
            }
        }

        if let Err(e) = self.file.close_rc().await {
            warn!(error = ?e, "Reader failed to close file");
        }
    }

    /// Spawns a new concurrent read task.
    fn spawn_reader(&self, ctx: ReadContext) {
        let waiter = self.global_limiter.clone();
        let file = self.file.clone();

        let task = ReadOpTask {
            file_key: self.file_key,
            ctx,
            waiter,
            file,
        };

        glommio::spawn_local(task.run()).detach();
    }
}

/// A single read operation for a given position and length.
struct ReadOpTask {
    file_key: FileKey,
    ctx: ReadContext,
    waiter: Arc<Semaphore>,
    file: Rc<DmaFile>,
}

impl ReadOpTask {
    #[instrument(
        "reader",
        skip_all,
        fields(
            file_key = ?self.file_key,
            pos = self.ctx.pos,
            len = self.ctx.len,
        )
    )]
    /// Runs the read operation.
    ///
    /// This may wait until there is availability to read from the global
    /// limiter. This is to prevent overloading of IO devices.
    async fn run(self) {
        let res = self.read().await;

        if let Err(ref e) = res {
            error!(error = ?e, "Failed to complete read op");
        }

        let _ = self.ctx.tx.send(res);
    }

    async fn read(&self) -> io::Result<ReadBuffer> {
        // Wait for our turn to read.
        let _permit = self.waiter.acquire().await;

        let result = self.file.read_at(self.ctx.pos as u64, self.ctx.len).await?;

        Ok(ReadBuffer::copy_from(result.as_ref()))
    }
}

struct ReadContext {
    pos: usize,
    len: usize,
    tx: oneshot::Sender<io::Result<ReadBuffer>>,
}

enum IoOp {
    Read { ctx: ReadContext },
    Close { tx: oneshot::Sender<io::Result<()>> },
}
