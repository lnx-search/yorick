use std::io;
use std::path::Path;
use std::sync::Arc;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

use crate::{BlobHeader, FileKey, WriteId};

#[derive(Clone)]
/// A buffered IO writer.
///
/// This internally wraps a tokio [File] and a [BufWriter].
pub struct Writer {
    file_key: FileKey,
    file: Arc<Mutex<WriterInner>>,
}

impl Writer {
    #[instrument("open-or-create-writer")]
    /// Opens or creates a given file located at the file path.
    pub(crate) async fn create(file_key: FileKey, path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .await?;

        if let Some(parent) = path.parent() {
            crate::backends::utils::sync_directory(parent).await?;
        }

        let inner = WriterInner {
            file_key,
            writer: BufWriter::new(file),
            cursor: 0,
        };

        Ok(Self {
            file_key,
            file: Arc::new(Mutex::new(inner)),
        })
    }

    #[instrument("writer", skip_all, fields(file_key = ?self.file_key))]
    /// Writes a given blob to the currently open file.
    pub async fn write_blob(
        &self,
        header: BlobHeader,
        buffer: &[u8],
    ) -> io::Result<WriteId> {
        let mut lock = self.file.lock().await;
        lock.write_blob(header, buffer).await.map_err(|e| {
            error!(error = ?e, "Failed to write blob");
            e
        })
    }

    #[instrument("writer", skip_all, fields(file_key = ?self.file_key))]
    /// Flushes the buffers of the writer to disk.
    pub async fn sync(&self) -> io::Result<()> {
        let mut lock = self.file.lock().await;
        lock.sync().await.map_err(|e| {
            error!(error = ?e, "Failed to flush data to disk");
            e
        })?;
        Ok(())
    }
}

struct WriterInner {
    file_key: FileKey,
    writer: BufWriter<File>,
    cursor: usize,
}

impl WriterInner {
    async fn write_blob(
        &mut self,
        header: BlobHeader,
        buffer: &[u8],
    ) -> io::Result<WriteId> {
        let header = header.as_bytes();
        self.writer.write_all(&header).await?;
        self.cursor += header.len();

        self.writer.write_all(buffer).await?;
        self.cursor += buffer.len();

        Ok(WriteId::new(self.file_key, self.cursor as u64))
    }

    async fn sync(&mut self) -> io::Result<()> {
        self.writer.get_mut().sync_data().await
    }
}
