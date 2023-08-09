#[macro_use]
extern crate tracing;

use std::hash::{Hash, Hasher};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rkyv::{Archive, Deserialize, Serialize};
use tokio::io;

use crate::index::IndexBackgroundSnapshotter;
use crate::read::{ReadContext, ReaderCache};
use crate::write::{WriteContext, WriterContext, WriterSizeController};

mod backends;
mod cleanup;
mod index;
mod init;
mod merge;
mod read;
mod write;

#[cfg(feature = "direct-io-backend")]
pub use self::backends::DirectIoConfig;
pub use self::backends::{BufferedIoConfig, FileReader, FileWriter, StorageBackend};
pub use self::index::{BlobIndex, BlobInfo};

/// The unique ID for a given blob.
pub type BlobId = u64;

#[derive(Debug)]
/// The configuration options for the storage service.
pub struct StorageServiceConfig {
    /// The base directory where all files are stored.
    pub base_path: PathBuf,
    /// The maximum size threshold for a file.
    ///
    /// If a file goes beyond this, a new writer will be created
    /// and the file will be sealed.
    ///
    /// Any file above this threshold will not be considered for compaction,
    /// only for delete cleanups.
    pub max_file_size: u64,
}

/// The primary storage service that controls reading and writing data.
pub struct YorickStorageService {
    /// The writer context.
    writer_context: WriterContext,
    /// The actor which controls automatic file rollover's kill switch.
    size_controller_stop_signal: Arc<AtomicBool>,
    /// The actor which creates snapshots of the in memory index.
    index_snapshot_stop_signal: Arc<AtomicBool>,
    /// The live blob lookup table/index.
    blob_index: BlobIndex,
    /// The live reader cache.
    readers: ReaderCache,
}

impl YorickStorageService {
    #[instrument("storage-service-create", skip(backend))]
    /// Creates a new storage service using a given backend.
    pub async fn create(
        backend: StorageBackend,
        config: StorageServiceConfig,
    ) -> io::Result<Self> {
        let data_directory = get_data_path(&config.base_path);
        let indexes_directory = get_index_path(&config.base_path);

        tokio::fs::create_dir_all(&data_directory).await?;
        tokio::fs::create_dir_all(&indexes_directory).await?;

        cleanup::cleanup_empty_files(&data_directory).await?;
        let next_file_key = init::load_next_file_key(&data_directory).await?;
        let current_snapshot_id =
            init::load_next_snapshot_id(&indexes_directory).await?;
        let blob_index =
            init::load_blob_index(&indexes_directory, &data_directory).await?;

        // Setup a new writer.
        let writer_path = get_data_file(&data_directory, next_file_key);
        let writer = backend.open_writer(next_file_key, &writer_path).await?;
        let writer_context = WriterContext::new(writer);

        let size_controller_stop_signal = WriterSizeController::spawn(
            config.max_file_size,
            next_file_key.0 + 1,
            data_directory.clone(),
            backend.clone(),
            writer_context.clone(),
        )
        .await;

        let index_snapshot_stop_signal = IndexBackgroundSnapshotter::spawn(
            current_snapshot_id,
            &indexes_directory,
            blob_index.clone(),
        );

        let readers = ReaderCache::new(backend, data_directory);

        Ok(Self {
            writer_context,
            size_controller_stop_signal,
            index_snapshot_stop_signal,
            blob_index,
            readers,
        })
    }

    /// Shuts down the manager.
    pub fn shutdown(&self) {
        self.index_snapshot_stop_signal
            .store(true, Ordering::Relaxed);
        self.size_controller_stop_signal
            .store(true, Ordering::Relaxed);
    }

    /// Creates a new write context for writing blobs of data.
    pub fn create_write_ctx(&self) -> WriteContext {
        WriteContext::new(&self.blob_index, &self.readers, self.writer_context.get())
    }

    /// Creates a read context for reading blobs.
    pub fn create_read_ctx(&self) -> ReadContext {
        ReadContext::new(&self.blob_index, &self.readers)
    }
}

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Archive, Serialize, Deserialize,
)]
/// A unique identifier for a given file.
pub struct FileKey(pub(crate) u32);
impl Hash for FileKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.0)
    }
}

#[derive(
    Debug,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Archive,
    Serialize,
    Deserialize,
)]
/// A unique cursor into the completed operation.
///
/// This ID is able to be sorted.
pub struct WriteId {
    /// Our file is an incrementing counter.
    ///
    /// If our key is bigger than another, we know our ID is more recent.
    file_key: FileKey,
    /// The position of the cursor at the point of creating the ID.
    end_pos: u64,
}

impl WriteId {
    pub(crate) fn new(file_key: FileKey, pos: u64) -> Self {
        Self {
            file_key,
            end_pos: pos,
        }
    }
}

/// A metadata header for each blob entry.
pub struct BlobHeader {
    /// The ID of the blob.
    pub blob_id: u64,
    /// The length of the blob in bytes.
    pub blob_length: u32,
    /// The group ID to connect blobs.
    pub group_id: u64,
    /// A crc32 checksum of the blob.
    pub checksum: u32,
}

impl BlobHeader {
    /// The static size of the header on disk.
    pub const SIZE: usize = mem::size_of::<u64>()
        + mem::size_of::<u32>()
        + mem::size_of::<u64>()
        + mem::size_of::<u32>();

    /// Returns the total length of the blob including the header.
    pub fn buffer_length(&self) -> usize {
        Self::SIZE + self.blob_length as usize
    }

    /// Returns the header as bytes.
    pub fn as_bytes(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0; Self::SIZE];
        buffer[0..8].copy_from_slice(&self.blob_id.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.blob_length.to_le_bytes());
        buffer[12..20].copy_from_slice(&self.group_id.to_le_bytes());
        buffer[20..24].copy_from_slice(&self.checksum.to_le_bytes());
        buffer
    }

    /// Creates a new header from a given buffer.
    pub fn from_bytes(buffer: [u8; Self::SIZE]) -> Self {
        Self {
            blob_id: u64::from_le_bytes(buffer[0..8].try_into().unwrap()),
            blob_length: u32::from_le_bytes(buffer[8..12].try_into().unwrap()),
            group_id: u64::from_le_bytes(buffer[12..20].try_into().unwrap()),
            checksum: u32::from_le_bytes(buffer[20..24].try_into().unwrap()),
        }
    }
}

/// The file extension for a data file.
pub static DATA_FILE_EXT: &str = "yrk1";
/// The file extension for a data file.
pub static INDEX_FILE_EXT: &str = "snap";

/// Returns the path of the data directory given a base path.
fn get_data_path(path: &Path) -> PathBuf {
    path.join("data")
}

/// Returns the path of the index snapshot directory given a base path.
fn get_index_path(path: &Path) -> PathBuf {
    path.join("index-snapshots")
}

/// Returns the path of the formatted data file.
fn get_data_file(base_path: &Path, file_key: FileKey) -> PathBuf {
    base_path
        .join(file_key.0.to_string())
        .with_extension(DATA_FILE_EXT)
}
/// Returns the path of the formatted index file.
fn get_snapshot_file(base_path: &Path, snapshot_id: u64) -> PathBuf {
    base_path
        .join(snapshot_id.to_string())
        .with_extension(INDEX_FILE_EXT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_id_sorting() {
        let id1 = WriteId::new(FileKey(1), 0);
        let id2 = WriteId::new(FileKey(2), 5);
        let id3 = WriteId::new(FileKey(2), 3);

        let mut entries = vec![id1, id2, id3];
        entries.sort();

        assert_eq!(entries, [id1, id3, id2], "Write IDs should sort correctly");
    }
}
