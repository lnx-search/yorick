use ahash::HashMap;

use crate::{BlobHeader, BlobId, FileKey};

/// A in-memory index for performing lookups matching `blob_id -> file + pos`.
///
/// This structure is kept purely in memory because the amount of memory
/// required is fairly minimal when
pub struct BlobIndex {
    index: HashMap<BlobId, BlobHeader>,
}

/// Metadata info about a specific blob.
pub struct BlobInfo {
    /// The unique file ID of where the blob is stored.
    file_key: FileKey,
    /// The start position in the file of the blob.
    pos: u64,
    /// The length of the blob.
    len: u32,
    /// The ID of the group the blob belongs to.
    group_id: u64,
}
