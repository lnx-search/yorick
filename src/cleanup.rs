use std::io;
use std::path::Path;

#[instrument("cleanup-empty-files")]
/// Attempts to remove any empty files located in the data directory.
pub async fn cleanup_empty_files(data_dir: &Path) -> io::Result<()> {
    let path = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || cleanup_empty_inner(&path))
        .await
        .expect("Spawn background thread")
}

fn cleanup_empty_inner(data_dir: &Path) -> io::Result<()> {
    let list_dir = data_dir.read_dir()?;

    for entry in list_dir {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            warn!(path = %path.display(), "Skipping directory located in data path");
            continue;
        }

        if let Err(e) = maybe_remove_file(&path) {
            warn!(error = ?e, "Failed to consider file cleanup due to error");
        } else {
            info!(path = %path.display(), "Cleaned up file");
        }
    }

    Ok(())
}

/// Attempts to remove the file at a given path if it is empty.
fn maybe_remove_file(path: &Path) -> io::Result<()> {
    let metadata = path.metadata()?;

    if metadata.len() == 0 {
        std::fs::remove_file(path)?;
    }

    Ok(())
}
