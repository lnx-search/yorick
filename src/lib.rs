#[macro_use]
extern crate tracing;

use std::hash::{Hash, Hasher};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rkyv::{Archive, Deserialize, Serialize};
use tokio::io;

use crate::compaction::{BlobCompactor, CompactorController};
use crate::index::IndexBackgroundSnapshotter;
use crate::read::{ReadContext, ReaderCache};
use crate::tools::{KillSwitch, LockingCounter};
use crate::write::{ActiveWriter, WriteContext, WriterSizeController};

mod backends;
mod cleanup;
mod compaction;
mod index;
mod init;
mod read;
mod tools;
mod write;

#[cfg(feature = "direct-io-backend")]
pub use self::backends::DirectIoConfig;
pub use self::backends::{
    BufferedIoConfig,
    FileReader,
    FileWriter,
    ReadBuffer,
    StorageBackend,
};
pub use self::compaction::{
    CompactionConfig,
    CompactionPolicy,
    DefaultCompactionPolicy,
    NoCompactionPolicy,
};
pub use self::index::{BlobIndex, BlobInfo};

/// The unique ID for a given blob.
pub type BlobId = u64;

#[derive(Debug, Clone)]
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

impl StorageServiceConfig {
    /// The location of the yorick data files.
    pub fn data_path(&self) -> PathBuf {
        get_data_path(&self.base_path)
    }

    /// The location of the index snapshot files.
    pub fn index_snapshot_path(&self) -> PathBuf {
        get_index_snapshot_path(&self.base_path)
    }

    /// Ensures all the directories are created for the given config.
    pub(crate) async fn ensure_directories_exist(&self) -> io::Result<()> {
        tokio::fs::create_dir_all(self.data_path()).await?;
        tokio::fs::create_dir_all(self.index_snapshot_path()).await?;
        Ok(())
    }
}

#[derive(Clone)]
/// The primary storage service that controls reading and writing data.
pub struct YorickStorageService {
    /// A hook which is triggered once all copies of the
    /// storage service are closed.
    shutdown_hook: Arc<ShutdownHooks>,
    /// The active writer context.
    active_writer: ActiveWriter,
    /// The live reader context.
    readers: ReadContext,
}

impl YorickStorageService {
    /// Creates a new storage service using a given backend.
    pub async fn create(
        backend: StorageBackend,
        config: StorageServiceConfig,
    ) -> io::Result<Self> {
        Self::create_with_compaction(backend, config, DefaultCompactionPolicy).await
    }

    #[instrument("storage-service-create", skip(backend, compaction_policy))]
    /// Creates a new storage service using a given backend and compaction policy.
    pub async fn create_with_compaction(
        backend: StorageBackend,
        config: StorageServiceConfig,
        compaction_policy: impl CompactionPolicy,
    ) -> io::Result<Self> {
        info!("Creating storage service");

        let start = Instant::now();
        let data_directory = config.data_path();
        let indexes_directory = config.index_snapshot_path();

        // Make sure our directory is setup.
        config.ensure_directories_exist().await?;
        info!("Directories OK");

        // Prune any files we dont need.
        cleanup::cleanup_empty_files(&data_directory).await?;
        info!("Directory cleanup OK");

        let next_file_key = init::load_next_file_key(&data_directory).await?;
        info!(next_file_key = ?next_file_key, "Last known position found");

        let current_snapshot_id =
            init::load_next_snapshot_id(&indexes_directory).await?;
        info!(
            current_snapshot_id = current_snapshot_id,
            "Found snapshot ID"
        );

        let blob_index =
            init::load_blob_index(&indexes_directory, &data_directory).await?;
        info!("Memory index re-created OK");

        // Setup the monotonic file key counter
        let file_key_counter = LockingCounter::new(next_file_key.0);
        let new_file_key = FileKey(file_key_counter.inc());

        // Create a new active writer.
        let writer_path = get_data_file(&data_directory, new_file_key);
        let writer = backend.open_writer(new_file_key, &writer_path).await?;
        let active_writer = ActiveWriter::new(writer);

        let size_controller_switch = WriterSizeController::spawn(
            config.max_file_size,
            file_key_counter.clone(),
            data_directory.clone(),
            backend.clone_internal(),
            active_writer.clone(),
        )
        .await;

        // Memory index snapshotter
        let index_snapshot_switch = IndexBackgroundSnapshotter::spawn(
            current_snapshot_id,
            &indexes_directory,
            blob_index.clone(),
        );

        // Create the reader cache.
        let reader_cache = ReaderCache::new(backend.clone_internal(), data_directory);
        let readers = ReadContext::new(blob_index.clone(), reader_cache);

        // Start the compaction actor with the given policy.
        let compaction_controller = BlobCompactor::spawn(
            Box::new(compaction_policy),
            file_key_counter.clone(),
            readers.clone(),
            config,
            backend.clone_internal(),
        )
        .await;

        let shutdown_hook = ShutdownHooks {
            is_shutdown: AtomicBool::new(false),
            backend,
            size_controller_switch,
            index_snapshot_switch,
            compaction_controller,
        };

        info!(elapsed = ?start.elapsed(), "Service has been setup");

        Ok(Self {
            shutdown_hook: Arc::new(shutdown_hook),
            active_writer,
            readers,
        })
    }

    /// Starts a new compaction cycle running.
    ///
    /// This method will complete once the trigger has been sent,
    /// but this does not mean the compaction has been completed yet.
    pub async fn start_compaction(&self) {
        self.shutdown_hook.compaction_controller.compact().await;
    }

    /// Creates a new write context for writing blobs of data.
    pub fn create_write_ctx(&self) -> WriteContext {
        WriteContext::new(self.readers.clone(), self.active_writer.get())
    }

    /// Creates a read context for reading blobs.
    pub fn create_read_ctx(&self) -> ReadContext {
        self.readers.clone()
    }

    /// Shuts down the service.
    ///
    /// This does *not* close the backend.
    pub fn shutdown(&self) -> io::Result<()> {
        self.shutdown_hook.shutdown()
    }
}

/// A struct for automatically triggering a shutdown when everything closes.
///
/// This is mostly to prevent some of the actors driven by interval timings
/// from running forever.
struct ShutdownHooks {
    /// A flag to signal if the system is already shutdown.
    is_shutdown: AtomicBool,
    /// The active IO backend.
    backend: StorageBackend,
    /// The controller for managing the current compactor.
    compaction_controller: CompactorController,
    /// The actor which controls automatic file rollover's kill switch.
    size_controller_switch: KillSwitch,
    /// The actor which creates snapshots of the in memory index.
    index_snapshot_switch: KillSwitch,
}

impl ShutdownHooks {
    /// Shuts down the service.
    ///
    /// This does *not* close the backend.
    fn shutdown(&self) -> io::Result<()> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.compaction_controller.kill();
        self.size_controller_switch.set_killed();
        self.index_snapshot_switch.set_killed();
        let res = self.backend.wait_shutdown();

        self.is_shutdown.store(true, Ordering::Relaxed);

        res
    }
}

impl Drop for ShutdownHooks {
    fn drop(&mut self) {
        if let Err(e) = self.shutdown() {
            error!(error = ?e, "Failed to complete shutdown");
        }
    }
}

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Archive, Serialize, Deserialize,
)]
#[archive(check_bytes)]
#[cfg_attr(test, derive(Default))]
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
#[archive(check_bytes)]
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
#[cfg_attr(test, derive(Eq, PartialEq))]
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
    /// The merge counter AKA number of merges this blob has been through.
    pub merge_counter: u32,
}

impl BlobHeader {
    /// The static size of the header on disk.
    pub const SIZE: usize = 2
        + mem::size_of::<u32>()
        + mem::size_of::<u64>()
        + mem::size_of::<u32>()
        + mem::size_of::<u64>()
        + mem::size_of::<u32>()
        + mem::size_of::<u32>();

    /// The static size of the header on disk.
    const SIZE_INFO_ONLY: usize = mem::size_of::<u64>()
        + mem::size_of::<u32>()
        + mem::size_of::<u64>()
        + mem::size_of::<u32>()
        + mem::size_of::<u32>();

    /// The magic bytes which signals to the handlers that the data after it might be a header.
    ///
    /// This is only used for recovering if corrupted data is discovered.
    const MAGIC_BYTES: [u8; 2] = [1, 1];

    #[inline]
    /// Creates a new blob header.
    pub fn new(blob_id: u64, blob_length: u32, group_id: u64, checksum: u32) -> Self {
        Self::new_with_merges(blob_id, blob_length, group_id, checksum, 0)
    }

    #[inline]
    /// Creates a new blob header with a given number of merges.
    pub fn new_with_merges(
        blob_id: u64,
        blob_length: u32,
        group_id: u64,
        checksum: u32,
        merge_counter: u32,
    ) -> Self {
        let mut buffer = [0; Self::SIZE_INFO_ONLY];

        buffer[0..8].copy_from_slice(&blob_id.to_le_bytes());
        buffer[8..12].copy_from_slice(&blob_length.to_le_bytes());
        buffer[12..20].copy_from_slice(&group_id.to_le_bytes());
        buffer[20..24].copy_from_slice(&checksum.to_le_bytes());
        buffer[24..28].copy_from_slice(&merge_counter.to_le_bytes());

        let blob_header_checksum = tools::stable_hash(&buffer);

        Self {
            blob_header_checksum,
            blob_id,
            blob_length,
            group_id,
            checksum,
            merge_counter,
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

    /// Returns the number of the merges the blob has been through.
    pub fn merge_counter(&self) -> usize {
        self.merge_counter as usize
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
        buffer[30..34].copy_from_slice(&self.merge_counter.to_le_bytes());
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
            merge_counter: u32::from_le_bytes(buffer[30..34].try_into().unwrap()),
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
fn get_index_snapshot_path(path: &Path) -> PathBuf {
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

    #[test]
    fn test_header_serialization() {
        let header = BlobHeader::new(1, 0, 3, 0);
        let bytes = header.as_bytes();
        assert_eq!(bytes.len(), 34);
        let recovered = BlobHeader::from_bytes(bytes);
        assert_eq!(recovered.unwrap(), header, "Headers should match");

        let header = BlobHeader::new_with_merges(0, 0, 0, 0, 55);
        let bytes = header.as_bytes();
        assert_eq!(bytes.len(), 34);
        let recovered = BlobHeader::from_bytes(bytes);
        assert_eq!(recovered.unwrap(), header, "Headers should match");
    }
}
