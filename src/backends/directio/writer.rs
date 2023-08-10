use std::io;
use std::path::{Path, PathBuf};

use futures_lite::AsyncWriteExt;
use glommio::io::{DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use tokio::sync::oneshot;

use super::WriteBuffer;
use crate::backends::directio::{lost_contact_error, Task};
use crate::{BlobHeader, FileKey, WriteId};

type WriteOpTx = tachyonix::Sender<IoOp>;
type WriteOpRx = tachyonix::Receiver<IoOp>;

pub struct WriterTask {
    pub(super) file_key: FileKey,
    pub(super) path: PathBuf,
    pub(super) tx: oneshot::Sender<io::Result<WriterMailbox>>,
}

#[async_trait::async_trait(?Send)]
impl Task for WriterTask {
    async fn spawn(self) {
        let (tx, rx) = tachyonix::channel(5);

        let res = WriterActor::create(self.file_key, &self.path, rx).await;

        match res {
            Ok(actor) => {
                glommio::spawn_local(actor.run()).detach();

                let mailbox = WriterMailbox { tx };
                let _ = self.tx.send(Ok(mailbox));
            },
            Err(e) => {
                let _ = self.tx.send(Err(e));
            },
        }
    }
}

#[derive(Clone)]
/// A mailbox for sending operations to the given writer.
pub struct WriterMailbox {
    tx: WriteOpTx,
}

impl WriterMailbox {
    /// Closes the currently open file.
    pub async fn close(&self) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send_op(IoOp::Close { tx }).await?;
        rx.await.map_err(|_| lost_contact_error())?
    }

    /// Flushes the buffers of the writer to disk, returning the position in bytes.
    pub async fn sync(&self) -> io::Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.send_op(IoOp::Sync { tx }).await?;
        rx.await.map_err(|_| lost_contact_error())?
    }

    /// Writes a given blob to the currently open file.
    pub async fn write_blob(
        &self,
        header: BlobHeader,
        buffer: WriteBuffer,
    ) -> io::Result<WriteId> {
        let (tx, rx) = oneshot::channel();
        self.send_op(IoOp::WriteBlob { header, buffer, tx }).await?;
        rx.await.map_err(|_| lost_contact_error())?
    }

    async fn send_op(&self, op: IoOp) -> io::Result<()> {
        self.tx.send(op).await.map_err(|_| lost_contact_error())
    }
}

macro_rules! maybe_log_err {
    ($res:expr) => {{
        if let Err(ref e) = $res {
            error!(error = ?e, "Failed to complete IO operation");
        }
    }};
}

/// The stream writer actor which runs on the glommio runtime.
pub struct WriterActor {
    file_key: FileKey,
    writer: DmaStreamWriter,
    ops: WriteOpRx,
    closed: bool,
}

impl WriterActor {
    #[instrument("open-or-create-writer")]
    /// Opens or creates a given file located at the file path.
    async fn create(file_key: FileKey, path: &Path, rx: WriteOpRx) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(path)
            .await?;

        if let Some(parent) = path.parent() {
            crate::backends::utils::sync_directory(parent).await?;
        }

        let writer = DmaStreamWriterBuilder::new(file)
            .with_write_behind(10)
            .with_buffer_size(128 << 10)
            .build();

        Ok(Self {
            file_key,
            writer,
            ops: rx,
            closed: false,
        })
    }

    #[instrument("writer", skip_all, fields(file_key = ?self.file_key))]
    async fn run(mut self) {
        while let Ok(op) = self.ops.recv().await {
            self.handle_op(op).await;

            if self.closed {
                return; // We dont want to try close the file again.
            }
        }

        if let Err(e) = self.writer.close().await {
            error!(error = ?e, "Failed to close writer");
        }
    }

    async fn handle_op(&mut self, op: IoOp) {
        match op {
            IoOp::WriteBlob { header, buffer, tx } => {
                let res = self.writer.write_all(&header.as_bytes()).await;
                maybe_log_err!(res);
                if let Err(e) = res {
                    let _ = tx.send(Err(e));
                    return;
                }

                let res = self.writer.write_all((*buffer).as_ref()).await;
                maybe_log_err!(res);
                if let Err(e) = res {
                    let _ = tx.send(Err(e));
                } else {
                    let id = WriteId::new(self.file_key, self.writer.current_pos());
                    let _ = tx.send(Ok(id));
                }
            },
            IoOp::Sync { tx } => {
                let res = self.writer.sync().await;
                maybe_log_err!(res);
                let _ = tx.send(res.map_err(|e| e.into()));
            },
            IoOp::Close { tx } => {
                let res = self.writer.close().await;
                maybe_log_err!(res);
                let _ = tx.send(res);
                self.closed = true;
            },
        }
    }
}

enum IoOp {
    WriteBlob {
        header: BlobHeader,
        buffer: WriteBuffer,
        tx: oneshot::Sender<io::Result<WriteId>>,
    },
    Sync {
        tx: oneshot::Sender<io::Result<u64>>,
    },
    Close {
        tx: oneshot::Sender<io::Result<()>>,
    },
}
