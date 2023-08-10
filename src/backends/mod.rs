use std::io;
use std::ops::Deref;
use std::path::Path;

use crate::{BlobHeader, FileKey};

mod buffered;
#[cfg(feature = "direct-io-backend")]
mod directio;
mod reader;
mod utils;
mod writer;

pub use self::buffered::BufferedIoConfig;
#[cfg(feature = "direct-io-backend")]
pub use self::directio::DirectIoConfig;
pub use self::reader::FileReader;
pub use self::writer::FileWriter;

#[cfg(not(feature = "aligned-reads"))]
/// The inner buffer used by reads with no alignment guarantees.
type ReadBufferInner = Vec<u8>;
#[cfg(feature = "aligned-reads")]
/// The inner buffer used by reads with a 16 byte alignment.
type ReadBufferInner = rkyv::AlignedVec;

#[derive(Clone)]
/// The core storage backend used for completing IO operations on disk.
pub struct StorageBackend {
    inner: StorageBackendInner,
}

impl StorageBackend {
    /// Creates a new buffered IO backend with a given config.
    pub async fn create_blocking_io(config: BufferedIoConfig) -> io::Result<Self> {
        let backend = buffered::BufferedIoBackend::create(config)?;
        Ok(Self {
            inner: StorageBackendInner::BufferedIo(backend),
        })
    }

    #[cfg(feature = "direct-io-backend")]
    /// Creates a new direct IO backend with a given config.
    pub async fn create_direct_io(config: DirectIoConfig) -> io::Result<Self> {
        let backend = directio::DirectIoBackend::create(config).await?;
        Ok(Self {
            inner: StorageBackendInner::DirectIo(backend),
        })
    }

    /// Opens a new file writer.
    ///
    /// If the file does not already exist, it is created.
    pub async fn open_writer(
        &self,
        file_key: FileKey,
        path: &Path,
    ) -> io::Result<FileWriter> {
        match &self.inner {
            StorageBackendInner::BufferedIo(backend) => backend
                .open_writer(file_key, path)
                .await
                .map(|writer| FileWriter::from_buffered(file_key, writer)),
            #[cfg(feature = "direct-io-backend")]
            StorageBackendInner::DirectIo(backend) => backend
                .open_writer(file_key, path)
                .await
                .map(|writer| FileWriter::from_direct(file_key, writer)),
        }
    }

    /// Opens an existing file for reading.
    pub async fn open_reader(
        &self,
        file_key: FileKey,
        path: &Path,
    ) -> io::Result<FileReader> {
        match &self.inner {
            StorageBackendInner::BufferedIo(backend) => backend
                .open_reader(file_key, path)
                .await
                .map(reader::FileReader::from),
            #[cfg(feature = "direct-io-backend")]
            StorageBackendInner::DirectIo(backend) => backend
                .open_reader(file_key, path)
                .await
                .map(reader::FileReader::from),
        }
    }
}

#[derive(Clone)]
/// The inner storage backend.
///
/// This is just an enum that is set based on the runtime configuration.
enum StorageBackendInner {
    /// The blocking IO backend.
    BufferedIo(buffered::BufferedIoBackend),
    #[cfg(feature = "direct-io-backend")]
    /// The direct IO backend.
    DirectIo(directio::DirectIoBackend),
}

#[derive(Debug, Clone)]
/// A owned read result of the blob.
pub struct ReadBuffer {
    inner: ReadBufferInner,
}

impl Deref for ReadBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AsRef<[u8]> for ReadBuffer {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl ReadBuffer {
    /// Creates a new read buffer from a given slice.
    pub(crate) fn copy_from(buffer: &[u8]) -> Self {
        let mut buf = ReadBufferInner::with_capacity(buffer.len() - BlobHeader::SIZE);
        buf.extend_from_slice(&buffer[BlobHeader::SIZE..]);
        Self { inner: buf }
    }
}
