use std::io;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::backends::buffered;
#[cfg(feature = "direct-io-backend")]
use crate::backends::{directio, WriteBuffer};
use crate::{BlobHeader, FileKey, WriteId};

#[derive(Clone)]
/// A cheap to clone writer for a file.
///
/// This writer only allows sequential writing, and although it is cheap to clone,
/// operations are not concurrent unlike reads, they are instead queued.
pub struct FileWriter {
    /// The file key of the writer.
    file_key: FileKey,
    /// Indicates if the file is closed or not.
    closed: Arc<AtomicBool>,
    /// The inner file writer.
    inner: FileWriterInner,
    /// The number of bytes currently written to the writer.
    ///
    /// Some bytes may still be in-flight.
    num_bytes: Arc<AtomicU64>,
}

impl FileWriter {
    #[inline]
    /// Returns the number of bytes written to the writer.
    pub fn size(&self) -> u64 {
        self.num_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    /// Returns the file key of the writer.
    pub fn file_key(&self) -> FileKey {
        self.file_key
    }

    fn inc_size(&self, n: usize) {
        self.num_bytes.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Writes a given blob to the currently open file.
    pub async fn write_blob<B>(
        &self,
        header: BlobHeader,
        buffer: B,
    ) -> io::Result<WriteId>
    where
        B: AsRef<[u8]> + Send + 'static,
    {
        self.check_file_not_closed()?;

        let buf = buffer.as_ref();
        let num_bytes = buf.len();

        let res = match &self.inner {
            FileWriterInner::Buffered(writer) => writer.write_blob(header, buf).await,
            #[cfg(feature = "direct-io-backend")]
            FileWriterInner::DirectIo(writer) => {
                let buffer = directio::WriteBuffer::new(buffer);
                writer.write_blob(header, buffer).await
            },
        };

        if res.is_ok() {
            self.inc_size(num_bytes);
        }

        res
    }

    /// Flushes any in-memory data to disk ensuring it is safely persisted.
    pub async fn sync(&self) -> io::Result<()> {
        self.check_file_not_closed()?;

        match &self.inner {
            FileWriterInner::Buffered(writer) => writer.sync().await,
            #[cfg(feature = "direct-io-backend")]
            FileWriterInner::DirectIo(writer) => {
                writer.sync().await?;
                Ok(())
            },
        }
    }

    /// Closes the writer, ensuring all data is written to disk before hand.
    ///
    /// Once a file is closed no more operations can be performed.
    pub async fn close(&self) -> io::Result<()> {
        self.check_file_not_closed()?;

        let res = match &self.inner {
            FileWriterInner::Buffered(writer) => writer.sync().await,
            #[cfg(feature = "direct-io-backend")]
            FileWriterInner::DirectIo(writer) => writer.close().await,
        };

        if res.is_ok() {
            self.closed.store(true, Ordering::Relaxed);
        }

        Ok(())
    }

    fn check_file_not_closed(&self) -> io::Result<()> {
        if self.closed.load(Ordering::Relaxed) {
            Err(io::Error::new(ErrorKind::Other, "The file is closed"))
        } else {
            Ok(())
        }
    }
}

impl FileWriter {
    pub(crate) fn from_buffered(file_key: FileKey, writer: buffered::Writer) -> Self {
        Self {
            file_key,
            inner: FileWriterInner::Buffered(writer),
            closed: Arc::new(AtomicBool::new(false)),
            num_bytes: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[cfg(feature = "direct-io-backend")]
impl FileWriter {
    pub(crate) fn from_direct(
        file_key: FileKey,
        writer: directio::WriterMailbox,
    ) -> Self {
        Self {
            file_key,
            inner: FileWriterInner::DirectIo(writer),
            closed: Arc::new(AtomicBool::new(false)),
            num_bytes: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[derive(Clone)]
enum FileWriterInner {
    Buffered(buffered::Writer),
    #[cfg(feature = "direct-io-backend")]
    DirectIo(directio::WriterMailbox),
}
