use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{io, mem};

use exponential_backoff::Backoff;
use parking_lot::RwLock;
use smallvec::SmallVec;
use tokio::time::interval;

use crate::read::ReadContext;
use crate::tools::{KillSwitch, LockingCounter};
use crate::{
    BlobHeader,
    BlobId,
    BlobInfo,
    FileKey,
    FileWriter,
    StorageBackend,
    WriteId,
};

/// An active write context that allows you to write blobs of data to the service.
pub struct WriteContext {
    did_op: bool,
    read_context: ReadContext,
    file_writer: FileWriter,
    queued_changes: SmallVec<[(BlobId, BlobInfo); 1]>,
    removals_changes: Vec<BlobId>,
}

impl WriteContext {
    pub(crate) fn new(reader: ReadContext, writer: FileWriter) -> Self {
        Self {
            did_op: false,
            read_context: reader,
            file_writer: writer,
            queued_changes: SmallVec::new(),
            removals_changes: Vec::new(),
        }
    }

    #[instrument("write-blob", skip(self, buffer))]
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
        let header = BlobHeader::new(
            id,
            buf.len() as u32,
            group_id,
            checksum,
        );
        self.write_header_and_data(header, buffer).await
    }

    /// Writes a header and blob to the storage service.
    pub(crate) async fn write_header_and_data<B>(
        &mut self,
        header: BlobHeader,
        buffer: B,
    ) -> io::Result<WriteId>
    where
        B: AsRef<[u8]> + Send + 'static,
    {
        let len = header.blob_length();
        let write_id = self.file_writer.write_blob(header, buffer).await?;

        let info = BlobInfo {
            file_key: write_id.file_key,
            start_pos: write_id.end_pos - len as u64,
            len: len as u32,
            group_id: header.group_id,
            checksum: header.checksum,
            merge_counter: header.merge_counter,
        };
        self.queued_changes.push((header.blob_id, info));

        self.did_op = true;

        Ok(write_id)
    }

    /// Queues a delete operation.
    ///
    /// This is INTERNAL USE ONLY.
    pub(crate) fn mark_delete(&mut self, blob_id: BlobId) {
        self.removals_changes.push(blob_id);
    }

    #[instrument("commit", skip(self))]
    /// Commits the submitted blob information.
    pub async fn commit(mut self) -> io::Result<()> {
        if self.did_op {
            self.file_writer.sync().await?;
            trace!("Commit complete");

            let file_key = self.file_writer.file_key();
            // Reflect the changes to readers.
            self.read_context.cache().forget(file_key);

            {
                let writer = self.read_context.blob_index().writer();
                let mut lock = writer.lock();
                for (blob_id, info) in self.queued_changes {
                    lock.update(blob_id, info);
                }
                for blob_id in self.removals_changes {
                    lock.remove_entry(blob_id);
                }
                lock.publish();
            }

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
    file_key_counter: LockingCounter,
    base_path: PathBuf,
    backend: StorageBackend,
    kill_switch: KillSwitch,
    active_writer: ActiveWriter,
}

impl WriterSizeController {
    /// Spawns a new writer size controller actor, returning a stop signal.
    pub async fn spawn(
        threshold: u64,
        file_key_counter: LockingCounter,
        base_path: PathBuf,
        backend: StorageBackend,
        active_writer: ActiveWriter,
    ) -> KillSwitch {
        let kill_switch = KillSwitch::new();

        let actor = Self {
            threshold,
            file_key_counter,
            base_path,
            backend,
            kill_switch: kill_switch.clone(),
            active_writer,
        };

        tokio::spawn(actor.run());

        kill_switch
    }

    #[instrument("writer-size-controller", skip_all)]
    async fn run(mut self) {
        let mut interval = interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            trace!("Checking file size of writer...");

            if self.kill_switch.is_killed() {
                debug!("Size controller got shutdown signal");
                break;
            }

            let size = self.active_writer.current_size();
            if size < self.threshold {
                trace!(
                    size = size,
                    threshold = self.threshold,
                    "File size is not above the required threshold to warrant swapping",
                );
                continue;
            }

            info!(
                size = size,
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

        info!("File size controller shutting down");
    }

    /// Creates a new writer and replaces the old (now full) writer.
    async fn create_new_writer(&mut self) -> io::Result<u32> {
        let new_file_key = self.file_key_counter.inc();

        let path = crate::get_data_file(&self.base_path, FileKey(new_file_key));
        let writer = self
            .backend
            .open_writer(FileKey(new_file_key), &path)
            .await?;

        let old_writer = self.active_writer.set_writer(writer);
        if let Err(e) = old_writer.sync().await {
            warn!(error = ?e, "Failed to sync old writer file due to error");
        }

        Ok(new_file_key)
    }
}

#[derive(Clone)]
pub(crate) struct ActiveWriter {
    /// The currently active file writer.
    active_writer: Arc<RwLock<FileWriter>>,
}

impl ActiveWriter {
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
