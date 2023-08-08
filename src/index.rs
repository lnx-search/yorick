use ahash::HashMap;

use crate::{BlobHeader, BlobId, FileKey, WriteId};

/// A in-memory index for performing lookups matching `blob_id -> file + pos`.
///
/// This structure is kept purely in memory because the amount of memory
/// required is fairly minimal when
pub struct BlobIndex {
    index: HashMap<BlobId, BlobHeader>,
}

impl BlobIndex {
    /// Produces and iterator over the blob info index.
    pub fn iter_blobs(&self) -> impl Iterator<Item = (&BlobId, &BlobHeader)> {
        self.index.iter()
    }
}

#[derive(Debug, Copy, Clone)]
/// Metadata info about a specific blob.
pub struct BlobInfo {
    /// The unique file ID of where the blob is stored.
    file_key: FileKey,
    /// The start position in the file of the blob.
    pos: u64,
    /// The length of the blob.
    len: u32,
    /// The original write ID of the blob.
    ///
    /// This is maintained across merges.
    write_id: WriteId,
    /// The ID of the group the blob belongs to.
    group_id: u64,
}

impl BlobInfo {
    #[inline]
    /// The unique file ID of where the blob is stored.
    pub fn current_file_key(&self) -> FileKey {
        self.file_key
    }

    #[inline]
    /// The start position in the file of the blob.
    pub fn pos(&self) -> u64 {
        self.pos
    }

    #[inline]
    /// The length of the blob.
    pub fn len(&self) -> u32 {
        self.len
    }

    #[inline]
    /// The initial write ID of when this blob was first written.
    ///
    /// This is maintained across any merges.
    pub fn initial_write_id(&self) -> WriteId {
        self.write_id
    }

    #[inline]
    /// The ID of the group this blob belongs to.
    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    /// Checks if the blob was written before the given write ID.
    pub fn is_before(&self, write_id: WriteId) -> bool {
        self.write_id < write_id
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;

    #[test]
    fn test_size() {
        dbg!(mem::size_of::<BlobInfo>());
    }
}
