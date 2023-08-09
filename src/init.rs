use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::{cmp, io};

use ahash::{HashMap, HashMapExt};

use crate::{
    get_data_file,
    BlobHeader,
    BlobIndex,
    BlobInfo,
    FileKey,
    DATA_FILE_EXT,
    INDEX_FILE_EXT,
};

/// Finds the next available file key.
pub async fn load_next_file_key(data_path: &Path) -> io::Result<FileKey> {
    let path = data_path.to_path_buf();
    tokio::task::spawn_blocking(move || load_next_file_key_inner(&path))
        .await
        .expect("Spawn background thread")
}

/// Finds the next available file key.
pub async fn load_next_snapshot_id(data_path: &Path) -> io::Result<u64> {
    let path = data_path.to_path_buf();
    tokio::task::spawn_blocking(move || get_last_snapshot_id(&path))
        .await
        .expect("Spawn background thread")
        .map(|v| v.map(|v| v.0 + 1).unwrap_or_default())
}

/// Loads the latest blob index.
///
/// This will load from an existing snapshot or re-create it from
/// the data files by scanning if required.
pub async fn load_blob_index(
    index_path: &Path,
    data_path: &Path,
) -> io::Result<BlobIndex> {
    let index_path = index_path.to_path_buf();
    let data_path = data_path.to_path_buf();
    tokio::task::spawn_blocking(move || load_blob_index_inner(&index_path, &data_path))
        .await
        .expect("Spawn background thread")
}

fn load_blob_index_inner(index_path: &Path, data_path: &Path) -> io::Result<BlobIndex> {
    let base_index = find_latest_index_snapshot(index_path)?.unwrap_or_default();

    let file_metadata = get_files_list(data_path)?
        .into_iter()
        .map(|(key, path)| {
            let metadata = path.metadata()?;
            Ok::<_, io::Error>((key, metadata.len()))
        })
        .collect::<Result<BTreeMap<FileKey, u64>, _>>()?;

    let mut max_file_positions =
        get_max_file_position_from_current_index(&base_index, file_metadata.len());

    // Make sure all our files are up to data in the data they reference.
    let mut files_to_scan = Vec::new();
    for (file_key, len) in file_metadata {
        if let Some(max_pos) = max_file_positions.remove(&file_key) {
            if max_pos < len {
                files_to_scan.push((file_key, max_pos));
            }
        } else {
            files_to_scan.push((file_key, 0));
        }
    }

    // Files which were in our index, but are no longer existing.
    for file_key in max_file_positions.into_keys() {
        warn!(
            file_key = ?file_key,
            "File no longer exists on disk, removing... \
            This may have been due to a merge operation, or manual interference"
        );

        base_index.remove_from_file(file_key);
    }

    for (file_key, start_from) in files_to_scan {
        let path = get_data_file(data_path, file_key);
        scan_file(file_key, &path, start_from, &base_index)?;
    }

    Ok(base_index)
}

fn scan_file(
    file_key: FileKey,
    path: &Path,
    mut start_from: u64,
    index: &BlobIndex,
) -> io::Result<()> {
    let file_len = path.metadata()?.len();
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let mut lock = index.writer().lock();
    while start_from < file_len {
        let header = match read_next(&mut reader) {
            Err(e) => {
                warn!(error = ?e, "File is partially corrupted, saving progress");
                lock.publish();
                return Ok(());
            },
            Ok(header) => header,
        };

        let info = BlobInfo {
            file_key,
            pos: start_from,
            len: (start_from + header.buffer_length() as u64) as u32,
            group_id: header.group_id,
        };

        lock.update(header.blob_id, info);

        // Advance the cursor.
        start_from += header.buffer_length() as u64;
    }

    lock.publish();

    Ok(())
}

fn read_next(file: &mut BufReader<File>) -> io::Result<BlobHeader> {
    let mut header = [0; BlobHeader::SIZE];

    file.read_exact(&mut header)?;

    let header = BlobHeader::from_bytes(header);

    let chunk_size = cmp::min(header.blob_length as usize, 32 << 10);
    let mut buffer = vec![0u8; chunk_size];
    let mut hasher = crc32fast::Hasher::new();
    let mut remaining = header.blob_length as usize;
    while remaining > 0 {
        let pos = cmp::min(chunk_size, remaining);
        file.read_exact(&mut buffer[..pos])?;
        hasher.update(&buffer[..pos]);
        remaining -= pos;
    }

    let actual_checksum = hasher.finalize();
    if actual_checksum != header.checksum {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Checksum missmatch, expected {} != actual {actual_checksum}",
                header.checksum
            ),
        ));
    }

    Ok(header)
}

fn get_max_file_position_from_current_index(
    index: &BlobIndex,
    size_hint: usize,
) -> HashMap<FileKey, u64> {
    let mut max_file_positions = HashMap::with_capacity(size_hint);
    {
        let guard = match index.reader().enter() {
            None => return HashMap::new(),
            Some(guard) => guard,
        };

        for (_, info) in guard.iter() {
            let info = info.get_one().unwrap();
            let end = info.pos() + info.len() as u64;

            max_file_positions
                .entry(info.current_file_key())
                .and_modify(|v| {
                    *v = cmp::max(*v, end);
                })
                .or_insert(end);
        }
    }

    max_file_positions
}

fn find_latest_index_snapshot(index_path: &Path) -> io::Result<Option<BlobIndex>> {
    let recent_snapshot = get_last_snapshot_id(index_path)?;

    let (key, path) = match recent_snapshot {
        None => return Ok(None),
        Some(info) => info,
    };

    let buffer = std::fs::read(path)?;
    let index = match BlobIndex::load_snapshot(buffer) {
        Err(e) => {
            warn!(error = ?e, "Existing index snapshot was invalid");
            return Ok(None);
        },
        Ok(index) => index,
    };

    info!(file_key = key, "Existing index snapshot has been loaded");

    Ok(Some(index))
}

fn get_last_snapshot_id(index_path: &Path) -> io::Result<Option<(u64, PathBuf)>> {
    let dir = index_path.read_dir()?;

    let mut keys = Vec::new();
    for entry in dir {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            continue;
        }

        match parse_snapshot_file_name(&path) {
            Some(key) => {
                keys.push((key, path));
            },
            None => {
                warn!(path = %path.display(), "Ignoring file due to invalid file name");
            },
        }
    }

    Ok(keys.into_iter().max_by_key(|v| v.0))
}

fn load_next_file_key_inner(data_path: &Path) -> io::Result<FileKey> {
    let file_list = get_files_list(data_path)?;

    let last_key = file_list.iter().map(|v| v.0).max().unwrap_or(FileKey(0));

    Ok(FileKey(last_key.0 + 1))
}

fn get_files_list(data_path: &Path) -> io::Result<Vec<(FileKey, PathBuf)>> {
    let dir = data_path.read_dir()?;

    let mut keys = Vec::new();
    for entry in dir {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            continue;
        }

        match parse_data_file_name(&path) {
            Some(key) => {
                keys.push((key, path));
            },
            None => {
                warn!(path = %path.display(), "Ignoring file due to invalid file name");
            },
        }
    }

    Ok(keys)
}

fn parse_data_file_name(path: &Path) -> Option<FileKey> {
    let name_c_str = path.file_name()?;
    let name_str = name_c_str
        .to_str()?
        .strip_suffix(DATA_FILE_EXT)?
        .strip_suffix('.')?;
    name_str.parse::<u32>().ok().map(FileKey)
}

fn parse_snapshot_file_name(path: &Path) -> Option<u64> {
    let name_c_str = path.file_name()?;
    let name_str = name_c_str
        .to_str()?
        .strip_suffix(INDEX_FILE_EXT)?
        .strip_suffix('.')?;
    name_str.parse::<u64>().ok()
}
