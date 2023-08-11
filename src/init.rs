use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::{cmp, io};

use ahash::{HashMap, HashMapExt};

use crate::tools::{parse_data_file_name, parse_snapshot_file_name};
use crate::{get_data_file, BlobHeader, BlobId, BlobIndex, BlobInfo, FileKey};

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

    // Load oldest key to newest key.
    files_to_scan.sort_by_key(|v| v.0);

    for (file_key, start_from) in files_to_scan {
        let path = get_data_file(data_path, file_key);

        let mut lock = base_index.writer().lock();
        scan_file(file_key, &path, start_from, |blob_id, info| {
            lock.update(blob_id, info);
        })?;
        lock.publish();
    }

    Ok(base_index)
}

fn scan_file<CB>(
    file_key: FileKey,
    path: &Path,
    mut cursor: u64,
    mut header_callback: CB,
) -> io::Result<()>
where
    CB: FnMut(BlobId, BlobInfo),
{
    let file_len = path.metadata()?.len();
    let mut file = File::open(path)?;

    if cursor > 0 {
        file.seek(SeekFrom::Start(cursor))?;
    }

    let mut reader = BufReader::with_capacity(10 << 20, file);

    while cursor < file_len {
        let initial_cursor = cursor;
        let maybe_header = read_next(&mut reader, &mut cursor, file_len)?;

        let (bytes_skipped, header) = match maybe_header {
            None => {
                warn!(
                    invalid_bytes_start = initial_cursor,
                    "Reached EOF while finding next header"
                );
                return Ok(());
            },
            Some(data) => data,
        };

        if bytes_skipped > 0 {
            warn!(
                bytes_skipped = bytes_skipped,
                start_pos = cursor,
                "Some data was skipped as it is corrupted"
            );
        }

        let info = BlobInfo {
            file_key,
            start_pos: cursor,
            len: header.total_length() as u32,
            group_id: header.group_id,
            checksum: header.checksum,
        };

        (header_callback)(header.blob_id, info);

        cursor += header.blob_length() as u64;
        reader.seek_relative(header.blob_length() as i64)?;
    }

    Ok(())
}

/// Finds the next valid blob header.
///
/// If the file is partially corrupted, this can take a while as
/// the recovery operation is certainly not fast nor optimised.
fn read_next(
    file: &mut BufReader<File>,
    cursor: &mut u64,
    file_size: u64,
) -> io::Result<Option<(u64, BlobHeader)>> {
    let mut offset = 0;
    let mut temp_buffer = [0; BlobHeader::SIZE];
    let mut num_bytes_skipped = 0;
    while *cursor < file_size {
        match file.read_exact(&mut temp_buffer[offset..]) {
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
            Ok(()) => {},
        }

        (*cursor) += (BlobHeader::SIZE - offset) as u64;

        if let Some(header) = BlobHeader::from_bytes(temp_buffer) {
            return Ok(Some((num_bytes_skipped, header)));
        }

        // If we can't read a valid header, move 1 byte across.
        temp_buffer.copy_within(1.., 0);
        offset = BlobHeader::SIZE - 1;

        num_bytes_skipped += 1;
    }

    Ok(None)
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
            let end = info.start_pos() + info.len() as u64;

            max_file_positions
                .entry(info.file_key())
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    static SAMPLE: &[u8] = b"Some, random, data";

    #[test]
    fn test_file_scan_simple() {
        let tmp_file = test_utils::temp_dir().join("test_file_scan_simple.data");

        let header = BlobHeader::new(1, 0, 1, crc32fast::hash(&[]));
        std::fs::write(&tmp_file, &header.as_bytes()).unwrap();

        let mut times_called = 0;
        scan_file(FileKey(1), &tmp_file, 0, |_, _| times_called += 1)
            .expect("Scan should be ok");
        assert_eq!(times_called, 1, "One header should be retrieved");

        std::fs::remove_dir_all(tmp_file.parent().unwrap()).unwrap();
    }

    #[test]
    fn test_file_scan_offset() {
        let tmp_file = test_utils::temp_dir().join("test_file_scan_offset.data");

        let header = BlobHeader::new(1, 0, 1, crc32fast::hash(&[]));
        let mut file = File::create(&tmp_file).unwrap();
        file.write_all(SAMPLE).unwrap();
        file.write_all(&header.as_bytes()).unwrap();
        file.sync_data().unwrap();

        let mut times_called = 0;
        scan_file(FileKey(1), &tmp_file, SAMPLE.len() as u64, |_, _| {
            times_called += 1
        })
        .expect("Scan should be ok");
        assert_eq!(times_called, 1, "One header should be retrieved");

        std::fs::remove_dir_all(tmp_file.parent().unwrap()).unwrap();
    }

    #[test]
    fn test_file_scan_many_simple() {
        let tmp_file = test_utils::temp_dir().join("test_file_scan_simple.data");

        let header1 = BlobHeader::new(1, 0, 1, crc32fast::hash(&[]));
        let header2 = BlobHeader::new(2, 0, 1, crc32fast::hash(&[]));
        let header3 = BlobHeader::new(3, 0, 1, crc32fast::hash(&[]));
        let mut file = File::create(&tmp_file).unwrap();
        file.write_all(&header1.as_bytes()).unwrap();
        file.write_all(&header2.as_bytes()).unwrap();
        file.write_all(&header3.as_bytes()).unwrap();
        file.sync_data().unwrap();

        let mut times_called = 0;
        scan_file(FileKey(1), &tmp_file, 0, |blob_id, _| {
            times_called += 1;
            assert!(
                [1, 2, 3].contains(&blob_id),
                "Blob ID should be in valid list."
            )
        })
        .expect("Scan should be ok");
        assert_eq!(times_called, 3, "Three headers should be retrieved");

        std::fs::remove_dir_all(tmp_file.parent().unwrap()).unwrap();
    }

    #[test]
    fn test_file_scan_many_offset() {
        let tmp_file = test_utils::temp_dir().join("test_file_scan_simple.data");

        let header1 = BlobHeader::new(1, 0, 1, crc32fast::hash(&[]));
        let header2 = BlobHeader::new(2, 0, 1, crc32fast::hash(&[]));
        let header3 = BlobHeader::new(3, 0, 1, crc32fast::hash(&[]));
        let mut file = File::create(&tmp_file).unwrap();
        file.write_all(SAMPLE).unwrap();
        file.write_all(&header1.as_bytes()).unwrap();
        file.write_all(&header2.as_bytes()).unwrap();
        file.write_all(&header3.as_bytes()).unwrap();
        file.sync_data().unwrap();

        let mut times_called = 0;
        scan_file(FileKey(1), &tmp_file, SAMPLE.len() as u64, |blob_id, _| {
            times_called += 1;
            assert!(
                [1, 2, 3].contains(&blob_id),
                "Blob ID should be in valid list."
            )
        })
        .expect("Scan should be ok");
        assert_eq!(times_called, 3, "Three headers should be retrieved");

        std::fs::remove_dir_all(tmp_file.parent().unwrap()).unwrap();
    }

    #[test]
    fn test_file_scan_corrupted_recover() {
        let tmp_file = test_utils::temp_dir().join("test_file_scan_simple.data");

        let header1 = BlobHeader::new(1, 0, 1, crc32fast::hash(&[]));
        let header2 = BlobHeader::new(2, 0, 1, crc32fast::hash(&[]));
        let header3 = BlobHeader::new(3, 0, 1, crc32fast::hash(&[]));
        let mut file = File::create(&tmp_file).unwrap();
        file.write_all(SAMPLE).unwrap();
        file.write_all(&header1.as_bytes()).unwrap();
        file.write_all(SAMPLE).unwrap();
        file.write_all(&header2.as_bytes()).unwrap();
        file.write_all(&header3.as_bytes()).unwrap();
        file.sync_data().unwrap();

        let mut times_called = 0;
        scan_file(FileKey(1), &tmp_file, SAMPLE.len() as u64, |blob_id, _| {
            times_called += 1;
            assert!(
                [1, 2, 3].contains(&blob_id),
                "Blob ID should be in valid list."
            )
        })
        .expect("Scan should be ok");
        assert_eq!(times_called, 3, "Three headers should be retrieved");

        let header1 = BlobHeader::new(1, 0, 1, crc32fast::hash(&[]));
        let header2 = BlobHeader::new(2, 0, 1, crc32fast::hash(&[]));
        let header3 = BlobHeader::new(3, 0, 1, crc32fast::hash(&[]));
        let mut file = File::create(&tmp_file).unwrap();
        file.write_all(SAMPLE).unwrap();
        file.write_all(&header1.as_bytes()).unwrap();
        file.write_all(&[1]).unwrap();
        file.write_all(&header2.as_bytes()).unwrap();
        file.write_all(&header3.as_bytes()).unwrap();
        file.sync_data().unwrap();

        let mut times_called = 0;
        scan_file(FileKey(1), &tmp_file, SAMPLE.len() as u64, |blob_id, _| {
            times_called += 1;
            assert!(
                [1, 2, 3].contains(&blob_id),
                "Blob ID should be in valid list."
            )
        })
        .expect("Scan should be ok");
        assert_eq!(times_called, 3, "Three headers should be retrieved");

        let header1 = BlobHeader::new(1, 0, 1, crc32fast::hash(&[]));
        let mut file = File::create(&tmp_file).unwrap();
        file.write_all(SAMPLE).unwrap();
        file.write_all(&header1.as_bytes()).unwrap();
        file.write_all(SAMPLE).unwrap();
        file.write_all(SAMPLE).unwrap();
        file.sync_data().unwrap();

        let mut times_called = 0;
        scan_file(FileKey(1), &tmp_file, SAMPLE.len() as u64, |blob_id, _| {
            times_called += 1;
            assert_eq!(blob_id, 1);
        })
        .expect("Scan should be ok");
        assert_eq!(times_called, 1, "One header should be retrieved");

        std::fs::remove_dir_all(tmp_file.parent().unwrap()).unwrap();
    }
}
