use std::io;
use std::path::Path;
use std::sync::Arc;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

use crate::{BlobHeader, FileKey};

#[derive(Clone)]
/// A buffered IO writer.
///
/// This internally wraps a tokio [File] and a [BufWriter].
pub struct Writer {
    file_key: FileKey,
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Writer {
    #[instrument("open-or-create-writer")]
    /// Opens or creates a given file located at the file path.
    pub(crate) async fn open_or_create(
        file_key: FileKey,
        path: &Path,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .await?;

        Ok(Self {
            file_key,
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    #[instrument("writer", skip_all, fields(file_key = ?self.file_key))]
    /// Writes a given blob to the currently open file.
    pub async fn write_blob(&self, header: BlobHeader, buffer: &[u8]) -> io::Result<()> {
        let mut lock = self.file.lock().await;
        lock.write_all(header.as_bytes()).await?;
        lock.write_all(buffer).await?;
        Ok(())
    }

    /// Flushes the buffers of the writer to disk.
    pub async fn sync(&self) -> io::Result<()> {
        let mut lock = self.file.lock().await;
        lock.get_mut().sync_data().await?;
        Ok(())
    }
}
