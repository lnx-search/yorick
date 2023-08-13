use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use arc_swap::ArcSwap;

use crate::backends::ReadBuffer;
use crate::{
    get_data_file,
    BlobId,
    BlobIndex,
    BlobInfo,
    FileKey,
    FileReader,
    StorageBackend,
};

#[derive(Clone)]
/// A context manager for performing read operations.
pub struct ReadContext {
    blob_index: BlobIndex,
    readers: ReaderCache,
}

impl ReadContext {
    pub(crate) fn new(index: BlobIndex, readers: ReaderCache) -> Self {
        Self {
            blob_index: index,
            readers,
        }
    }

    /// Gets access to the reader cache.
    pub(crate) fn cache(&self) -> ReaderCache {
        self.readers.clone()
    }

    #[inline]
    /// Returns a reference to the blob index.
    pub fn blob_index(&self) -> &BlobIndex {
        &self.blob_index
    }

    /// Read a blob from the service.
    ///
    /// This is treated as a random read operation, a reader will be created if
    /// it is not already open.
    ///
    /// The operation returns the `ReadResult` if the blob exists which contains
    /// the relevant metadata and buffer data.
    pub async fn read_blob(&self, blob_id: BlobId) -> io::Result<Option<ReadResult>> {
        let info = match self.blob_index.get(blob_id) {
            None => return Ok(None),
            Some(info) => info,
        };

        let reader = self.readers.get_or_create(info.file_key).await?;
        reader
            .read_at(info.start_pos() as usize, info.total_length() as usize)
            .await
            .map(|data| Some(ReadResult { info, data }))
    }
}

/// The read data and blob info from a read request.
pub struct ReadResult {
    /// The metadata info about the blob
    pub info: BlobInfo,
    /// The raw data of the blob that does *not* include the blob header.
    pub data: ReadBuffer,
}

/// A cache of live readers which are open.
///
/// This manages opening new readers, and closing old readers
/// which may have stale data.
pub(crate) struct ReaderCache {
    live_readers: Arc<ArcSwap<HashMap<FileKey, FileReader>>>,
    backend: StorageBackend,
    base_path: Arc<PathBuf>,
}

impl Clone for ReaderCache {
    fn clone(&self) -> Self {
        Self {
            live_readers: self.live_readers.clone(),
            backend: self.backend.clone_internal(),
            base_path: self.base_path.clone(),
        }
    }
}

impl ReaderCache {
    pub(crate) fn new(backend: StorageBackend, base_path: PathBuf) -> Self {
        Self {
            live_readers: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            backend,
            base_path: Arc::new(base_path),
        }
    }

    #[instrument("reader-cache", skip(self))]
    /// Tells the cache to forget about a specific reader.
    pub(crate) fn forget(&self, file_key: FileKey) {
        let guard = self.live_readers.load();
        let remove = if let Some(reader) = guard.get(&file_key) {
            reader.needs_manual_reload()
        } else {
            false
        };

        if remove {
            debug!(file_key = ?file_key, "Forgetting reader");
            self.live_readers.rcu(|readers| {
                let mut readers = HashMap::clone(readers);
                readers.remove(&file_key);
                readers
            });
        }
    }

    #[instrument("reader-cache", skip(self))]
    /// Attempts to get an existing, open reader or creates a new reader.
    pub(crate) async fn get_or_create(
        &self,
        file_key: FileKey,
    ) -> io::Result<FileReader> {
        let guard = self.live_readers.load();

        if let Some(reader) = guard.get(&file_key).cloned() {
            return Ok(reader);
        }

        let path = get_data_file(&self.base_path, file_key);
        let reader = self.backend.open_reader(file_key, &path).await?;

        self.live_readers.rcu(|readers| {
            let mut readers = HashMap::clone(readers);
            readers.insert(file_key, reader.clone());
            readers
        });

        Ok(reader)
    }
}
