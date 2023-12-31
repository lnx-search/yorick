use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::random_state::RandomState;
use evmap::StableHashEq;
use exponential_backoff::Backoff;
use parking_lot::Mutex;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

use crate::tools::KillSwitch;
use crate::{get_snapshot_file, BlobHeader, BlobId, FileKey, WriteId};

type ReadHandle = evmap::handles::ReadHandle<BlobId, BlobInfo, (), RandomState>;
type WriteHandle = evmap::handles::WriteHandle<BlobId, BlobInfo, (), RandomState>;

#[derive(Debug, thiserror::Error)]
/// An error that occurs when attempting to load a snapshot of the [BlobIndex].
pub enum LoadSnapshotError {
    #[error("Not enough data is present for the snapshot to be valid")]
    NotEnoughBytes,
    #[error("The expected checksum ({expected}) does not match the actual ({actual}) checksum of the data")]
    ChecksumMissmatch { expected: u32, actual: u32 },
    #[error("The data is invalid and cannot be read")]
    InvalidData,
}

#[derive(Clone)]
/// The lookup index for mapping blob Ids to their location and info on disk.
pub struct BlobIndex {
    writer: Arc<Mutex<WriteHandle>>,
    reader: ReadHandle,
    has_changed: Arc<AtomicU64>,
}

impl Default for BlobIndex {
    /// Creates a new, blank blob index.
    fn default() -> Self {
        let (writer, reader) = create_reader_writer_map(0);

        Self {
            writer: Arc::new(Mutex::new(writer)),
            reader,
            has_changed: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl BlobIndex {
    #[cfg(test)]
    /// Inserts a single blob ID into the index with some info.
    pub(crate) fn insert_many(
        &self,
        iter: impl IntoIterator<Item = (BlobId, BlobInfo)>,
    ) {
        let mut lock = self.writer.lock();
        for (blob_id, info) in iter {
            if let Some(existing) = lock.get_one(&blob_id) {
                // We already have newer data.
                if existing.file_key() > info.file_key {
                    continue;
                }
            }

            lock.update(blob_id, info);
        }
        lock.publish();
        self.has_changed.fetch_add(1, Ordering::Relaxed);
    }

    /// Removes multiple blobs from the index.
    pub(crate) fn remove_many(&self, iter: impl IntoIterator<Item = BlobId>) {
        let mut lock = self.writer.lock();
        for blob_id in iter {
            lock.remove_entry(blob_id);
        }
        lock.publish();
        self.has_changed.fetch_add(1, Ordering::Relaxed);
    }

    /// Removes all keys that are apart of a given file.
    pub(crate) fn remove_from_file(&self, file_key: FileKey) {
        let keys = self.get_keys_for_file(file_key);
        self.remove_many(keys);
        self.has_changed.fetch_add(1, Ordering::Relaxed);
    }

    /// Gets all keys that are apart of a given file.
    pub(crate) fn get_keys_for_file(&self, file_key: FileKey) -> Vec<BlobId> {
        self.reader
            .enter()
            .iter()
            .flatten()
            .filter_map(|(key, info)| {
                let info = info.get_one().unwrap();

                if info.file_key == file_key {
                    Some(*key)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns a reference to the index reader.
    pub(crate) fn reader(&self) -> &ReadHandle {
        &self.reader
    }

    /// Returns a reference to the index writer.
    pub(crate) fn writer(&self) -> &Mutex<WriteHandle> {
        &self.writer
    }

    /// Gets information about a given blob ID if it exists.
    pub fn get(&self, blob_id: BlobId) -> Option<BlobInfo> {
        self.reader.get_one(&blob_id).map(|v| *v)
    }

    /// Serializes the current index and compresses it (if enabled via the `compress-index` feature).
    pub fn create_snapshot(&self) -> Vec<u8> {
        let entries: Vec<_> = self.reader.map_into(|key, value| KeyValuePair {
            key: *key,
            value: *value.get_one().unwrap(),
        });

        const SCRATCH_SPACE: usize = 64 << 10;
        let mut buffer = rkyv::to_bytes::<_, SCRATCH_SPACE>(&IndexData(entries))
            .expect("Serializing should be infallible");

        let checksum = crc32fast::hash(&buffer);
        buffer.extend_from_slice(&checksum.to_le_bytes());

        #[cfg(feature = "compress-index")]
        {
            let mut buffer = lz4_flex::compress_prepend_size(&buffer);
            buffer.push(1); // Indicate we're compressed
            buffer
        }

        #[cfg(not(feature = "compress-index"))]
        {
            let mut buffer = buffer.into_vec();
            buffer.push(0); // Indicate we're not compressed
            buffer
        }
    }

    /// Loads an index from a given snapshot buffer.
    ///
    /// This will automatically decompress the buffer if it is compressed
    /// and the `compress-index` feature is enabled.
    ///
    /// In the event the buffer is compressed and the feature is *not* enabled,
    /// this method will panic.
    pub fn load_snapshot(mut buffer: Vec<u8>) -> Result<Self, LoadSnapshotError> {
        let compressed_flag = buffer.pop().ok_or(LoadSnapshotError::NotEnoughBytes)?;

        if compressed_flag == 1 {
            #[cfg(feature = "compress-index")]
            {
                buffer = lz4_flex::decompress_size_prepended(&buffer)
                    .map_err(|_| LoadSnapshotError::InvalidData)?;
            }
            #[cfg(not(feature = "compress-index"))]
            panic!("Attempting to load a compressed snapshot, but the `compress-index` feature is not enabled.");
        }

        let d = buffer.pop().ok_or(LoadSnapshotError::NotEnoughBytes)?;
        let c = buffer.pop().ok_or(LoadSnapshotError::NotEnoughBytes)?;
        let b = buffer.pop().ok_or(LoadSnapshotError::NotEnoughBytes)?;
        let a = buffer.pop().ok_or(LoadSnapshotError::NotEnoughBytes)?;
        let expected_checksum = u32::from_le_bytes([a, b, c, d]);

        let actual_checksum = crc32fast::hash(&buffer);
        if expected_checksum != actual_checksum {
            return Err(LoadSnapshotError::ChecksumMissmatch {
                actual: actual_checksum,
                expected: expected_checksum,
            });
        }

        let mut aligned_buffer = AlignedVec::with_capacity(buffer.len());
        aligned_buffer.extend_from_slice(&buffer);
        drop(buffer);

        // SAFETY: This is safe as our alignment is to 16 bytes from the buffer, and our checksums
        // matched correctly.
        let view: &rkyv::Archived<IndexData> =
            unsafe { rkyv::archived_root::<IndexData>(&aligned_buffer) };

        let (mut writer, reader) = create_reader_writer_map(view.0.len());
        for pair in view.0.iter() {
            let pair: &rkyv::Archived<KeyValuePair> = pair;

            let value = pair.value.deserialize(&mut rkyv::Infallible).unwrap();
            writer.insert(pair.key.value(), value);
        }

        writer.publish();

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            reader,
            has_changed: Arc::new(AtomicU64::new(0)),
        })
    }
}

#[derive(Archive, Serialize, Deserialize)]
struct IndexData(#[with(rkyv::with::CopyOptimize)] Vec<KeyValuePair>);

#[repr(C)]
#[derive(Copy, Clone, Archive, Serialize, Deserialize)]
/// A helper struct because tuples are not repr(c) safe.
struct KeyValuePair {
    key: BlobId,
    value: BlobInfo,
}

#[repr(C)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Archive, Serialize, Deserialize)]
#[cfg_attr(test, derive(Default))]
/// Metadata info about a specific blob.
pub struct BlobInfo {
    /// The unique file ID of where the blob is stored.
    pub(crate) file_key: FileKey,
    /// The start position in the file of the blob.
    pub(crate) start_pos: u64,
    /// The length of the blob INCLUDING the header.
    pub(crate) total_length: u32,
    /// The ID of the group the blob belongs to.
    pub(crate) group_id: u64,
    /// The checksum of the blob data.
    pub(crate) checksum: u32,
    /// The number of merges the blob has gone through.
    pub(crate) merge_counter: u32,
}

impl BlobInfo {
    pub(crate) fn using_write_id(
        write_id: WriteId,
        blob_length: u32,
        group_id: u64,
        checksum: u32,
        merge_counter: u32,
    ) -> Self {
        let start_pos = write_id.end_pos - blob_length as u64 - BlobHeader::SIZE as u64;
        let len = write_id.end_pos - start_pos;
        Self {
            file_key: write_id.file_key,
            start_pos,
            total_length: len as u32,
            group_id,
            checksum,
            merge_counter,
        }
    }

    #[inline]
    /// The unique file ID of where the blob is stored.
    pub fn file_key(&self) -> FileKey {
        self.file_key
    }

    #[inline]
    /// The start position of the blob data in the file.
    pub fn start_pos(&self) -> u64 {
        self.start_pos
    }

    #[inline]
    /// The end position of the blob data in the file.
    pub fn end_pos(&self) -> u64 {
        self.start_pos + self.total_length as u64
    }

    #[inline]
    /// The length of the blob + header.
    pub fn total_length(&self) -> u32 {
        self.total_length
    }

    #[inline]
    /// The length of the blob.
    pub fn blob_length(&self) -> u32 {
        self.total_length - BlobHeader::SIZE as u32
    }

    #[inline]
    /// Returns if the blob is empty
    pub fn is_empty(&self) -> bool {
        self.total_length == 0
    }

    #[inline]
    /// The ID of the group this blob belongs to.
    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    #[inline]
    /// Returns the number of merges the blob has been through
    pub fn merge_counter(&self) -> usize {
        self.merge_counter as usize
    }

    /// Checks if the blob was written before the given write ID.
    pub fn is_before(&self, write_id: WriteId) -> bool {
        WriteId::new(self.file_key, self.start_pos) < write_id
    }
}

/// Our blob info is copy safe and always hashes to the same thing regardless of how it's cloned.
unsafe impl StableHashEq for BlobInfo {}

fn create_reader_writer_map(capacity: usize) -> (WriteHandle, ReadHandle) {
    // SAFETY:
    // This is safe as ahash's RandomState provides us with a deterministic input
    // like the standard SipHasher.
    unsafe {
        evmap::Options::default()
            .with_hasher(RandomState::new())
            .with_capacity(capacity)
            .construct()
    }
}

/// A background task that produces snapshots after some changes have been made.
pub(crate) struct IndexBackgroundSnapshotter {
    next_snapshot_id: u64,
    base_path: PathBuf,
    kill_switch: KillSwitch,
    index: BlobIndex,
    last_cleanup: Instant,
}

impl IndexBackgroundSnapshotter {
    pub(crate) fn spawn(
        current_snapshot_id: u64,
        base_path: &Path,
        index: BlobIndex,
    ) -> KillSwitch {
        let kill_switch = KillSwitch::new();
        let actor = Self {
            next_snapshot_id: current_snapshot_id + 1,
            base_path: base_path.to_path_buf(),
            index,
            kill_switch: kill_switch.clone(),
            last_cleanup: Instant::now(),
        };

        std::thread::Builder::new()
            .name("yorick-snapshot-thread".to_string())
            .spawn(move || actor.run())
            .expect("Spawn background thread");

        kill_switch
    }

    #[instrument("index-snapshot", skip_all)]
    fn run(mut self) {
        let mut last_counter = 0;
        loop {
            std::thread::sleep(Duration::from_millis(750));

            if self.kill_switch.is_killed() {
                break;
            }

            let changed_counter = self.index.has_changed.load(Ordering::Relaxed);
            if changed_counter <= last_counter {
                continue;
            }

            let backoff =
                Backoff::new(3, Duration::from_secs(1), Duration::from_secs(5));
            let mut attempt = 0;
            for backoff in &backoff {
                attempt += 1;
                match self.try_snapshot() {
                    Ok(()) => {
                        debug!(
                            attempt = attempt,
                            snapshot_id = self.next_snapshot_id - 1,
                            "Created snapshot"
                        );
                        last_counter = changed_counter;
                        break;
                    },
                    Err(e) => {
                        error!(
                            attempt = attempt,
                            error = ?e,
                            "Failed to create index snapshot",
                        );
                        std::thread::sleep(backoff);
                    },
                }
            }
        }
    }

    fn try_snapshot(&mut self) -> io::Result<()> {
        let next_id = self.next_snapshot_id;
        self.next_snapshot_id += 1;

        let snapshot = self.index.create_snapshot();
        let path = get_snapshot_file(&self.base_path, next_id);

        let mut file = std::fs::File::create(&path)?;
        file.write_all(&snapshot)?;
        file.sync_all()?;

        if let Some(parent) = path.parent() {
            sync_directory(parent)?;
        }

        if let Err(e) = self.try_cleanup_old_files(&path) {
            warn!(error = ?e, "Failed to cleanup old snapshot files");
        }

        Ok(())
    }

    fn try_cleanup_old_files(&mut self, exclude_file: &Path) -> io::Result<()> {
        // We only really need to cleanup files every so often.
        if self.last_cleanup.elapsed() < Duration::from_secs(10) {
            return Ok(());
        }

        let dir = self.base_path.read_dir()?;

        for entry in dir {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                warn!(path = %path.display(), "Ignoring directory in snapshot directory");
                continue;
            }

            if path != exclude_file {
                if let Err(e) = std::fs::remove_file(&path) {
                    warn!(path = %path.display(), error = ?e, "Failed to cleanup old files");
                }
            }
        }

        self.last_cleanup = Instant::now();

        Ok(())
    }
}

#[cfg(unix)]
/// A runtime agnostic directory sync.
fn sync_directory(dir: &Path) -> io::Result<()> {
    use std::fs::File;

    let file = File::open(dir)?;
    file.sync_data()?;

    Ok(())
}

// Windows has no sync directory call like we do on unix.
#[cfg(windows)]
/// A runtime agnostic directory sync.
fn sync_directory(_dir: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_basic_functionality() {
        let index = BlobIndex::default();
        index.insert_many([
            (
                1,
                BlobInfo {
                    file_key: FileKey(1),
                    start_pos: 0,
                    total_length: 3,
                    group_id: 0,
                    checksum: 0,
                    merge_counter: 0,
                },
            ),
            (
                2,
                BlobInfo {
                    file_key: FileKey(1),
                    start_pos: 0,
                    total_length: 3,
                    group_id: 0,
                    checksum: 0,
                    merge_counter: 0,
                },
            ),
            (
                3,
                BlobInfo {
                    file_key: FileKey(1),
                    start_pos: 0,
                    total_length: 3,
                    group_id: 0,
                    checksum: 0,
                    merge_counter: 0,
                },
            ),
        ]);

        assert!(index.get(1).is_some(), "Blob should exist");
        assert!(index.get(2).is_some(), "Blob should exist");
        assert!(index.get(3).is_some(), "Blob should exist");
        assert!(index.get(4).is_none(), "Blob should NOT exist");

        index.remove_many([2]);

        assert!(index.get(1).is_some(), "Blob should exist");
        assert!(index.get(2).is_none(), "Blob should NOT exist");
        assert!(index.get(3).is_some(), "Blob should exist");
        assert!(index.get(4).is_none(), "Blob should NOT exist");
    }

    #[test]
    fn test_index_snapshot() {
        let index = BlobIndex::default();
        index.insert_many([
            (
                1,
                BlobInfo {
                    file_key: FileKey(1),
                    start_pos: 0,
                    total_length: 3,
                    group_id: 0,
                    checksum: 0,
                    merge_counter: 0,
                },
            ),
            (
                2,
                BlobInfo {
                    file_key: FileKey(1),
                    start_pos: 0,
                    total_length: 3,
                    group_id: 0,
                    checksum: 0,
                    merge_counter: 0,
                },
            ),
            (
                5,
                BlobInfo {
                    file_key: FileKey(1),
                    start_pos: 0,
                    total_length: 3,
                    group_id: 0,
                    checksum: 0,
                    merge_counter: 0,
                },
            ),
        ]);

        let snapshot = index.create_snapshot();
        let loaded =
            BlobIndex::load_snapshot(snapshot).expect("Reading snapshot should be OK");
        assert!(loaded.get(1).is_some(), "Blob should exist");
        assert!(loaded.get(2).is_some(), "Blob should exist");
        assert!(loaded.get(5).is_some(), "Blob should exist");
        assert!(loaded.get(4).is_none(), "Blob should NOT exist");
    }
}
