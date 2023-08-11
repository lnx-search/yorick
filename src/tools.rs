use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ahash::HashSet;
use parking_lot::{Mutex, RwLock};

use crate::{FileKey, DATA_FILE_EXT, INDEX_FILE_EXT};

#[derive(Clone)]
pub struct KillSwitch {
    shutdown_signal: Arc<AtomicBool>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self {
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_killed(&self) -> bool {
        self.shutdown_signal.load(Ordering::Relaxed)
    }

    pub fn set_killed(&self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
    }
}

/// Gets the city-32 hash of some bytes.
///
/// This is mostly meant for small checksums.
pub fn stable_hash(buf: &[u8]) -> u32 {
    cityhasher::hash(buf)
}

pub fn parse_data_file_name(path: &Path) -> Option<FileKey> {
    let name_c_str = path.file_name()?;
    let name_str = name_c_str
        .to_str()?
        .strip_suffix(DATA_FILE_EXT)?
        .strip_suffix('.')?;
    name_str.parse::<u32>().ok().map(FileKey)
}

pub fn parse_snapshot_file_name(path: &Path) -> Option<u64> {
    let name_c_str = path.file_name()?;
    let name_str = name_c_str
        .to_str()?
        .strip_suffix(INDEX_FILE_EXT)?
        .strip_suffix('.')?;
    name_str.parse::<u64>().ok()
}

#[derive(Debug, Clone)]
/// A counter that is behind a mutex.
///
/// This is done in place of an atomic to guarantee that the incrementing is
/// correct, for things like file IDs.
pub struct LockingCounter {
    inner: Arc<Mutex<u32>>,
}

impl LockingCounter {
    /// Create a new locking counter
    pub fn new(start: u32) -> Self {
        Self {
            inner: Arc::new(Mutex::new(start)),
        }
    }

    /// Increments the counter returning the *old* ID.
    pub fn inc(&self) -> u32 {
        let mut lock = self.inner.lock();
        let n = *lock;
        (*lock) += 1;
        n
    }
}

#[derive(Clone, Default)]
/// A structure for tracking currently active writers.
pub struct ActiveWriterTracker {
    active: Arc<RwLock<HashSet<FileKey>>>,
}

impl ActiveWriterTracker {
    /// Returns a hashset containing the active writers.
    pub fn get_active(&self) -> HashSet<FileKey> {
        self.active.read().clone()
    }

    /// Creates a new tracking ref.
    pub fn create_tracker(&self, file_key: FileKey) -> TrackingRef {
        self.active.write().insert(file_key);
        TrackingRef(Arc::new(Tracker {
            iam: file_key,
            active: self.active.clone(),
        }))
    }
}

#[derive(Clone)]
pub struct TrackingRef(Arc<Tracker>);

/// A tacker which will mark the writer as inactive when dropped.
struct Tracker {
    iam: FileKey,
    active: Arc<RwLock<HashSet<FileKey>>>,
}

impl Drop for Tracker {
    fn drop(&mut self) {
        self.active.write().remove(&self.iam);
    }
}
