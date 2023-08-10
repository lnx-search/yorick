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
