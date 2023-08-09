use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use arc_swap::ArcSwap;

use crate::backends::ReadBuffer;
use crate::{get_data_file, BlobId, BlobIndex, FileKey, FileReader, StorageBackend};

pub struct ReadContext<'a> {
    blob_index: &'a BlobIndex,
    readers: &'a ReaderCache,
}

impl<'a> ReadContext<'a> {
    pub(crate) fn new(index: &'a BlobIndex, readers: &'a ReaderCache) -> Self {
        Self {
            blob_index: index,
            readers,
        }
    }

    /// Read a blob from the service.
    pub async fn read_blob(&self, blob_id: BlobId) -> io::Result<Option<ReadBuffer>> {
        let info = match self.blob_index.get(blob_id) {
            None => return Ok(None),
            Some(info) => info,
        };

        let reader = self.readers.get_or_create(info.file_key).await?;
        reader
            .read_at(info.pos as usize, info.len as usize)
            .await
            .map(Some)
    }
}

pub(crate) struct ReaderCache {
    live_readers: Arc<ArcSwap<HashMap<FileKey, FileReader>>>,
    backend: StorageBackend,
    base_path: PathBuf,
}

impl ReaderCache {
    pub(crate) fn new(backend: StorageBackend, base_path: PathBuf) -> Self {
        Self {
            live_readers: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            backend,
            base_path,
        }
    }

    /// Attempts to get an existing, open reader or creates a new reader.
    async fn get_or_create(&self, file_key: FileKey) -> io::Result<FileReader> {
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
