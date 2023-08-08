#[macro_use]
extern crate tracing;

use std::hash::{Hash, Hasher};

mod backends;
mod index;
mod merge;

#[cfg(feature = "direct-io-backend")]
pub use backends::DirectIoConfig;
pub use backends::{BufferedIoConfig, FileReader, FileWriter, StorageBackend};

/// The unique ID for a given blob.
pub type BlobId = u64;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
/// A unique identifier for a given file.
pub struct FileKey(pub(crate) u16);
impl Hash for FileKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u16(self.0)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
/// A unique cursor into the completed operation.
///
/// This ID is able to be sorted.
pub struct WriteId {
    /// Our file is an incrementing counter.
    ///
    /// If our key is bigger than another, we know our ID is more recent.
    file_key: FileKey,
    /// The position of the cursor at the point of creating the ID.
    pos: usize,
}

impl WriteId {
    pub(crate) fn new(file_key: FileKey, pos: usize) -> Self {
        Self { file_key, pos }
    }
}

/// A metadata header for each blob entry.
pub struct BlobHeader {}
impl BlobHeader {
    pub fn as_bytes(&self) -> &[u8] {
        todo!()
    }
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
