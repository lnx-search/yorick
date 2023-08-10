use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct KillSwitch {
    shutdown_signal: Arc<AtomicBool>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self {
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_killed(&self) -> bool {
        self.shutdown_signal.load(Ordering::Relaxed)
    }

    pub fn set_killed(&self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
    }
}

/// Gets the city-32 hash of some bytes.
///
/// This is mostly meant for small checksums.
pub fn stable_hash(buf: &[u8]) -> u32 {
    cityhasher::hash(buf)
}
