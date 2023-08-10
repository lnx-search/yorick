use std::path::PathBuf;

use uuid::Uuid;

/// Creates a new temp directory for tests.
pub fn temp_dir() -> PathBuf {
    let path = std::env::temp_dir().join(Uuid::new_v4().to_string());

    std::fs::create_dir_all(&path).unwrap();

    path
}

/// Attempts to init a tracing logger for testing.
pub fn init_logging() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug")
    }

    let _ = tracing_subscriber::fmt::try_init();
}
