#[macro_use]
extern crate tracing;

use std::hash::{Hash, Hasher};

mod backends;
mod file_filter;

#[cfg(feature = "direct-io-backend")]
pub use backends::DirectIoConfig;
pub use backends::{BufferedIoConfig, FileReader, FileWriter, StorageBackend};

#[derive(Debug, Copy, Clone)]
/// A unique identifier for a given file.
pub struct FileKey(u64);
impl Hash for FileKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0)
    }
}

pub struct BlobHeader {}

impl BlobHeader {
    pub fn as_bytes(&self) -> &[u8] {
        todo!()
    }
}
