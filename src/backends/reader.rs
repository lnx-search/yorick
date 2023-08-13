use std::io;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[cfg(feature = "direct-io-backend")]
use crate::backends::directio;
use crate::backends::{buffered, ReadBuffer};

#[derive(Clone)]
/// A cheap to clone reader for a file.
///
/// This is optimised for random-access not sequential access.
pub struct FileReader {
    closed: Arc<AtomicBool>,
    inner: FileReaderInner,
}

impl FileReader {
    #[instrument("read-at", skip(self))]
    /// Performs a random read on the file at a given position, reading upto `len` bytes.
    pub async fn read_at(&self, pos: usize, len: usize) -> io::Result<ReadBuffer> {
        self.check_file_not_closed()?;

        trace!("Read bytes");

        match &self.inner {
            FileReaderInner::Buffered(reader) => reader.read_at(pos, len).await,
            #[cfg(feature = "direct-io-backend")]
            FileReaderInner::DirectIo(reader) => reader.read_at(pos, len).await,
        }
    }

    #[instrument("reader-close", skip(self))]
    /// Closes the currently open file.
    ///
    /// Once a file is closed no more operations can be completed on it and will
    /// begin to return errors.
    pub async fn close(&self) -> io::Result<()> {
        self.check_file_not_closed()?;

        trace!("Closing reader");

        let res = match &self.inner {
            #[cfg(feature = "direct-io-backend")]
            FileReaderInner::DirectIo(reader) => reader.close().await,
            _ => Ok(()),
        };

        if res.is_ok() {
            self.closed.store(true, Ordering::Relaxed);
        }

        res
    }

    #[inline]
    /// Returns if the reader needs to be manually re-created in order to reflect new data.
    pub fn needs_manual_reload(&self) -> bool {
        matches!(self.inner, FileReaderInner::Buffered(_))
    }

    fn check_file_not_closed(&self) -> io::Result<()> {
        if self.closed.load(Ordering::Relaxed) {
            Err(io::Error::new(ErrorKind::Other, "The file is closed"))
        } else {
            Ok(())
        }
    }
}

impl From<buffered::Reader> for FileReader {
    fn from(reader: buffered::Reader) -> Self {
        Self {
            inner: FileReaderInner::Buffered(reader),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[cfg(feature = "direct-io-backend")]
impl From<directio::ReaderMailbox> for FileReader {
    fn from(reader: directio::ReaderMailbox) -> Self {
        Self {
            inner: FileReaderInner::DirectIo(reader),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone)]
enum FileReaderInner {
    Buffered(buffered::Reader),
    #[cfg(feature = "direct-io-backend")]
    DirectIo(directio::ReaderMailbox),
}
