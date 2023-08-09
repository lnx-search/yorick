use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{io, mem};

use exponential_backoff::Backoff;
use parking_lot::RwLock;
use smallvec::SmallVec;
use tokio::time::interval;

use crate::read::ReaderCache;
use crate::{
    BlobHeader,
    BlobId,
    BlobIndex,
    BlobInfo,
    FileKey,
    FileWriter,
    StorageBackend,
    WriteId,
};

/// An active write context that allows you to write blobs of data to the service.
pub struct WriteContext<'a> {
    did_op: bool,
    blob_index: &'a BlobIndex,
    readers_cache: &'a ReaderCache,
    writer_context: FileWriter,
    queued_changes: SmallVec<[(BlobId, BlobInfo); 1]>,
}

impl<'a> WriteContext<'a> {
    pub(crate) fn new(
        index: &'a BlobIndex,
        readers: &'a ReaderCache,
        writer: FileWriter,
    ) -> Self {
        Self {
            did_op: false,
            blob_index: index,
            readers_cache: readers,
            writer_context: writer,
            queued_changes: SmallVec::new(),
        }
    }

    /// Writes a blob to the storage service.
    ///
    /// Each blob can be tagged with an ID and group ID.
    pub async fn write_blob<B>(
        &mut self,
        id: BlobId,
        group_id: u64,
        buffer: B,
    ) -> io::Result<WriteId>
    where
        B: AsRef<[u8]> + Send + 'static,
    {
        let buf = buffer.as_ref();
        assert!(
            buf.len() < u32::MAX as usize,
            "Buffer cannot be bigger than u32::MAX bytes"
        );

        let checksum = crc32fast::hash(buf);
        let header = BlobHeader {
            blob_id: id,
            blob_length: buf.len() as u32,
            group_id,
            checksum,
        };

        let len = header.buffer_length() as u32;
        let write_id = self.writer_context.write_blob(header, buffer).await?;

        let info = BlobInfo {
            file_key: write_id.file_key,
            start_pos: write_id.end_pos - len as u64,
            len,
            group_id,
        };
        self.queued_changes.push((id, info));

        self.did_op = true;

        Ok(write_id)
    }

    /// Commits the submitted blob information.
    pub async fn commit(mut self) -> io::Result<()> {
        if self.did_op {
            self.writer_context.sync().await?;

            let file_key = self.writer_context.file_key();
            // Reflect the changes to readers.
            self.readers_cache.forget(file_key);

            self.blob_index.insert_many(self.queued_changes);
            self.did_op = false;
        }
        Ok(())
    }
}

/// A background actor that watches the
/// current active writer, and changes to a new writer
/// once one file has reached a threshold.
pub(crate) struct WriterSizeController {
    threshold: u64,
    next_file_key: u32,
    base_path: PathBuf,
    backend: StorageBackend,
    stop_signal: Arc<AtomicBool>,
    writer_context: WriterContext,
}

impl WriterSizeController {
    /// Spawns a new writer size controller actor, returning a stop signal.
    pub async fn spawn(
        threshold: u64,
        next_file_key: u32,
        base_path: PathBuf,
        backend: StorageBackend,
        writer_context: WriterContext,
    ) -> Arc<AtomicBool> {
        let stop_signal = Arc::new(AtomicBool::new(false));

        let actor = Self {
            threshold,
            next_file_key,
            base_path,
            backend,
            stop_signal: stop_signal.clone(),
            writer_context,
        };

        tokio::spawn(actor.run());

        stop_signal
    }

    #[instrument("writer-size-controller", skip_all)]
    async fn run(mut self) {
        let mut interval = interval(Duration::from_secs(1));
        loop {
            interval.tick().await;

            if self.stop_signal.load(Ordering::Relaxed) {
                break;
            }

            let size = self.writer_context.current_size();
            if size < self.threshold {
                continue;
            }

            info!(
                size = size,
                next_file_key = self.next_file_key,
                "Writer is full, preparing to create new writer",
            );

            let backoff =
                Backoff::new(4, Duration::from_millis(500), Duration::from_secs(5));
            let mut attempt = 0;
            for backoff in &backoff {
                attempt += 1;
                match self.create_new_writer().await {
                    Ok(new_file_key) => {
                        info!(
                            new_file_key = new_file_key,
                            next_file_key = self.next_file_key,
                            "New writer has been created",
                        );

                        break;
                    },
                    Err(e) => {
                        error!(
                            error = ?e,
                            attempt = attempt,
                            "Failed to create new writer, retrying in {backoff:?}",
                        );
                    },
                }
            }
        }
    }

    /// Creates a new writer and replaces the old (now full) writer.
    async fn create_new_writer(&mut self) -> io::Result<u32> {
        let new_file_key = self.next_file_key;
        self.next_file_key += 1;

        let path = crate::get_data_file(&self.base_path, FileKey(new_file_key));
        let writer = self
            .backend
            .open_writer(FileKey(new_file_key), &path)
            .await?;

        let old_writer = self.writer_context.set_writer(writer);
        if let Err(e) = old_writer.sync().await {
            warn!(error = ?e, "Failed to sync old writer file due to error");
        }

        Ok(new_file_key)
    }
}

#[derive(Clone)]
pub(crate) struct WriterContext {
    /// The currently active file writer.
    active_writer: Arc<RwLock<FileWriter>>,
}

impl WriterContext {
    pub(crate) fn new(writer: FileWriter) -> Self {
        Self {
            active_writer: Arc::new(RwLock::new(writer)),
        }
    }

    pub(crate) fn current_size(&self) -> u64 {
        self.active_writer.read().size()
    }

    pub(crate) fn get(&self) -> FileWriter {
        self.active_writer.read().clone()
    }

    pub(crate) fn set_writer(&self, writer: FileWriter) -> FileWriter {
        mem::replace(&mut self.active_writer.write(), writer)
    }
}
