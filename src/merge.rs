use crate::WriteId;

/// A merge policy tells Yorick how it should compact the storage.
///
/// It allows you to effectively mutate the data how ever you like,
/// but internally yorick will pair up blobs based on size, aiming
/// to create gradually bigger blobs.
pub trait MergePolicy {
    /// Returns the write ID which the merger is allow to consider merging files up to.
    fn get_safe_merge_checkpoint(&self) -> Option<WriteId>;
}
