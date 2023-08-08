use std::hash::{BuildHasher, Hasher};

#[derive(Default)]
/// A hasher which performs no hashing.
///
/// You must ensure the value is only a u64 value
/// and is a high quality hash.
pub struct NoOpHasher(u64);

impl Hasher for NoOpHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let val = u64::from_le_bytes(bytes.try_into().unwrap());
        self.0 = val;
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

#[derive(Default, Copy, Clone)]
/// A random state which produces a [NoOpHasher]
pub struct NoOpRandomState;

impl BuildHasher for NoOpRandomState {
    type Hasher = NoOpHasher;

    fn build_hasher(&self) -> Self::Hasher {
        NoOpHasher::default()
    }
}
