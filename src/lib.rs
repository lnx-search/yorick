#[macro_use]
extern crate tracing;

use std::hash::{Hash, Hasher};
use std::mem;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rkyv::{Archive, Deserialize, Serialize};
use tokio::io;

use crate::index::IndexBackgroundSnapshotter;
use crate::read::{ReadContext, ReaderCache};
use crate::tools::KillSwitch;
use crate::write::{WriteContext, WriterContext, WriterSizeController};

mod backends;
mod cleanup;
mod index;
mod init;
mod merge;
mod read;
mod tools;
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

#[derive(Clone)]
/// The primary storage service that controls reading and writing data.
pub struct YorickStorageService {
    /// The active IO backend.
    backend: StorageBackend,
    /// The writer context.
    writer_context: WriterContext,
    /// The actor which controls automatic file rollover's kill switch.
    size_controller_switch: KillSwitch,
    /// The actor which creates snapshots of the in memory index.
    index_snapshot_switch: KillSwitch,
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
        info!("Creating storage service");

        let start = Instant::now();
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

        let size_controller_switch = WriterSizeController::spawn(
            config.max_file_size,
            next_file_key.0 + 1,
            data_directory.clone(),
            backend.clone(),
            writer_context.clone(),
        )
        .await;

        let index_snapshot_switch = IndexBackgroundSnapshotter::spawn(
            current_snapshot_id,
            &indexes_directory,
            blob_index.clone(),
        );

        let readers = ReaderCache::new(backend.clone(), data_directory);

        info!(elapsed = ?start.elapsed(), "Service has been setup");

        Ok(Self {
            backend,
            writer_context,
            size_controller_switch,
            index_snapshot_switch,
            blob_index,
            readers,
        })
    }

    /// Shuts down the manager.
    pub fn shutdown(&self) -> io::Result<()> {
        self.size_controller_switch.set_killed();
        self.index_snapshot_switch.set_killed();
        self.backend.wait_shutdown()
    }

    /// Creates a new write context for writing blobs of data.
    pub fn create_write_ctx(&self) -> WriteContext {
        WriteContext::new(
            self.blob_index.clone(),
            self.readers.clone(),
            self.writer_context.get(),
        )
    }

    /// Creates a read context for reading blobs.
    pub fn create_read_ctx(&self) -> ReadContext {
        ReadContext::new(self.blob_index.clone(), self.readers.clone())
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

    #[inline]
    /// The file key of where the data is written.
    pub fn file_key(&self) -> FileKey {
        self.file_key
    }

    #[inline]
    /// The end position of the blob.
    pub fn end_pos(&self) -> u64 {
        self.end_pos
    }
}

#[derive(Debug, Copy, Clone)]
/// A metadata header for each blob entry.
pub struct BlobHeader {
    /// A city hash checksum of the blob header.
    blob_header_checksum: u32,
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
    pub const SIZE: usize = 2
        + mem::size_of::<u32>()
        + mem::size_of::<u64>()
        + mem::size_of::<u32>()
        + mem::size_of::<u64>()
        + mem::size_of::<u32>();

    /// The static size of the header on disk.
    const SIZE_INFO_ONLY: usize = mem::size_of::<u64>()
        + mem::size_of::<u32>()
        + mem::size_of::<u64>()
        + mem::size_of::<u32>();

    /// The magic bytes which signals to the handlers that the data after it might be a header.
    ///
    /// This is only used for recovering if corrupted data is discovered.
    const MAGIC_BYTES: [u8; 2] = [1, 1];

    /// Creates a new blob header.
    pub fn new(blob_id: u64, blob_length: u32, group_id: u64, checksum: u32) -> Self {
        let mut buffer = [0; Self::SIZE_INFO_ONLY];

        buffer[0..8].copy_from_slice(&blob_id.to_le_bytes());
        buffer[8..12].copy_from_slice(&blob_length.to_le_bytes());
        buffer[12..20].copy_from_slice(&group_id.to_le_bytes());
        buffer[20..24].copy_from_slice(&checksum.to_le_bytes());

        let blob_header_checksum = tools::stable_hash(&buffer);

        Self {
            blob_header_checksum,
            blob_id,
            blob_length,
            group_id,
            checksum,
        }
    }

    /// Returns the total length of the blob including the header.
    pub fn total_length(&self) -> usize {
        Self::SIZE + self.blob_length as usize
    }

    /// Returns the length of the blob excluding the header.
    pub fn blob_length(&self) -> usize {
        self.blob_length as usize
    }

    /// Returns the header as bytes.
    pub fn as_bytes(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0; Self::SIZE];
        buffer[0..2].copy_from_slice(&Self::MAGIC_BYTES);
        buffer[2..6].copy_from_slice(&self.blob_header_checksum.to_le_bytes());
        buffer[6..14].copy_from_slice(&self.blob_id.to_le_bytes());
        buffer[14..18].copy_from_slice(&self.blob_length.to_le_bytes());
        buffer[18..26].copy_from_slice(&self.group_id.to_le_bytes());
        buffer[26..30].copy_from_slice(&self.checksum.to_le_bytes());
        buffer
    }

    /// Creates a new header from a given buffer.
    ///
    /// If the buffer is an invalid header, `None` is returned.
    pub fn from_bytes(buffer: [u8; Self::SIZE]) -> Option<Self> {
        if buffer[0..2] != Self::MAGIC_BYTES {
            return None;
        }

        let expected_checksum = u32::from_le_bytes(buffer[2..6].try_into().unwrap());
        let actual_checksum = tools::stable_hash(&buffer[6..]);

        if expected_checksum != actual_checksum {
            return None;
        }

        Some(Self {
            blob_header_checksum: actual_checksum,
            blob_id: u64::from_le_bytes(buffer[6..14].try_into().unwrap()),
            blob_length: u32::from_le_bytes(buffer[14..18].try_into().unwrap()),
            group_id: u64::from_le_bytes(buffer[18..26].try_into().unwrap()),
            checksum: u32::from_le_bytes(buffer[26..30].try_into().unwrap()),
        })
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
