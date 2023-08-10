use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;
use tokio::sync::oneshot;

use crate::backends::ReadBuffer;
use crate::FileKey;

#[derive(Clone)]
/// A buffered IO reader.
///
/// In reality, this implementation lies a bit, it uses a mmap internally
/// and hopes the operating system handles caching well enough.
pub struct Reader {
    file_key: FileKey,
    len: usize,
    map: Arc<Mmap>,
    pool: Arc<rayon::ThreadPool>,
}

impl Reader {
    #[instrument("open-reader")]
    /// Opens a new file for reading.
    pub(super) fn open(
        file_key: FileKey,
        path: &Path,
        pool: Arc<rayon::ThreadPool>,
    ) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len() as usize;
        let map = unsafe { Mmap::map(&file)? };

        #[cfg(unix)]
        {
            use memmap2::Advice;
            map.advise(Advice::Random)?;
        }

        Ok(Self {
            file_key,
            len,
            map: Arc::new(map),
            pool,
        })
    }

    #[instrument("reader", skip(self), fields(file_key = ?self.file_key))]
    /// Performs a random read on the file at the given position reading `len` bytes.
    pub async fn read_at(&self, pos: usize, len: usize) -> io::Result<ReadBuffer> {
        let (tx, rx) = oneshot::channel();

        let file_length = self.len;
        let map = self.map.clone();
        self.pool.spawn(move || {
            if pos >= file_length {
                let err = Err(io::Error::new(
                    ErrorKind::Other,
                    "Position in file outside of length of file.",
                ));
                let _ = tx.send(err);
                return;
            }

            let slice = &map[pos..pos + len];
            let buffer = ReadBuffer::copy_from(slice);
            let _ = tx.send(Ok(buffer));
        });

        rx.await.expect("Threadpool should never die").map_err(|e| {
            error!(error = ?e, "Failed to perform read op");
            e
        })
    }
}
