use std::io;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::backends::buffered;
#[cfg(feature = "direct-io-backend")]
use crate::backends::{directio, WriteBuffer};
use crate::{BlobHeader, WriteId};

#[derive(Clone)]
/// A cheap to clone writer for a file.
///
/// This writer only allows sequential writing, and although it is cheap to clone,
/// operations are not concurrent unlike reads, they are instead queued.
pub struct FileWriter {
    closed: Arc<AtomicBool>,
    inner: FileWriterInner,
}

impl FileWriter {
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

        match &self.inner {
            FileWriterInner::Buffered(writer) => {
                writer.write_blob(header, buffer.as_ref()).await
            },
            #[cfg(feature = "direct-io-backend")]
            FileWriterInner::DirectIo(writer) => {
                let buffer = directio::WriteBuffer::new(buffer);
                writer.write_blob(header, buffer).await
            },
        }
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

impl From<buffered::Writer> for FileWriter {
    fn from(writer: buffered::Writer) -> Self {
        Self {
            inner: FileWriterInner::Buffered(writer),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[cfg(feature = "direct-io-backend")]
impl From<directio::WriterMailbox> for FileWriter {
    fn from(writer: directio::WriterMailbox) -> Self {
        Self {
            inner: FileWriterInner::DirectIo(writer),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone)]
enum FileWriterInner {
    Buffered(buffered::Writer),
    #[cfg(feature = "direct-io-backend")]
    DirectIo(directio::WriterMailbox),
}
