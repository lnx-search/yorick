use std::collections::BTreeSet;
use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::{HashMap, HashMapExt, HashSet};

use crate::backends::ReadBuffer;
use crate::read::{ReadContext, ReaderCache};
use crate::tools::LockingCounter;
use crate::write::WriteContext;
use crate::{
    get_data_file,
    BlobHeader,
    BlobId,
    BlobInfo,
    FileKey,
    StorageBackend,
    StorageServiceConfig,
};

#[derive(Debug, Copy, Clone)]
/// The configuration of the compaction system.
pub struct CompactionConfig {
    /// The duration between scans being scheduled.
    ///
    /// You may want to increase/decrease the duration depending on
    /// your ingestion rate, as compactions are incremental
    /// and can require multiple passes.
    ///
    /// Defaults to `30s`.
    pub scan_interval: Duration,
    /// Returns if the compaction policy needs to read
    /// the blob data in order to consider a blob for deletion.
    ///
    /// Setting this to `false` can save IO bandwidth as it avoids
    /// the read handlers.
    ///
    /// Defaults to `false`.
    pub needs_data_for_delete: bool,
    /// The default concurrency to use when reading blobs.
    ///
    /// Defaults to `4`.
    pub read_concurrency: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(30),
            needs_data_for_delete: false,
            read_concurrency: 4,
        }
    }
}

/// A merge policy tells Yorick how it should compact the storage.
///
/// It allows you to effectively mutate the data how ever you like,
/// but internally yorick will pair up files based on size, aiming
/// to create gradually bigger files.
pub trait CompactionPolicy: Send + Sync + 'static {
    /// Get the current latest compaction config.
    fn get_config(&self) -> CompactionConfig {
        CompactionConfig::default()
    }

    /// Returns the file key which the merger is allowed to consider compacting files up to.
    ///
    /// This means combining smaller files into bigger files.
    fn get_safe_compact_checkpoint(&self) -> Option<FileKey>;

    /// Called when a new merge file is created, passing in the key of the merge file.
    fn on_merge_file_creation(&self, _file_key: FileKey) {}

    /// Returns if a given blob can be deleted.
    ///
    /// No data will be provided if the `needs_data_for_delete` flag is not enabled in the config.
    ///
    /// This method is ran in a background thread.
    fn can_delete(
        &self,
        blob_id: BlobId,
        info: BlobInfo,
        data: Option<&ReadBuffer>,
    ) -> bool;
}

/// A compaction policy which performs no compaction.
///
/// This performs no compaction.
pub struct NoCompactionPolicy;

impl CompactionPolicy for NoCompactionPolicy {
    fn get_safe_compact_checkpoint(&self) -> Option<FileKey> {
        None
    }

    fn can_delete(
        &self,
        _blob_id: BlobId,
        _info: BlobInfo,
        _data: Option<&ReadBuffer>,
    ) -> bool {
        false
    }
}

/// The default compaction policy.
///
/// This performs compaction of files which are not currently open.
pub struct DefaultCompactionPolicy;

impl CompactionPolicy for DefaultCompactionPolicy {
    fn get_safe_compact_checkpoint(&self) -> Option<FileKey> {
        Some(FileKey(u32::MAX))
    }

    fn can_delete(
        &self,
        _blob_id: BlobId,
        _info: BlobInfo,
        _data: Option<&ReadBuffer>,
    ) -> bool {
        false
    }
}

#[derive(Clone)]
/// The compactor kill switch.
pub struct CompactorController {
    tx: tachyonix::Sender<()>,
}

impl CompactorController {
    /// Trigger a compaction event on the controller.
    ///
    /// This method will complete once the trigger has been sent,
    /// but this does not mean the compaction has been completed yet.
    pub async fn compact(&self) {
        let _ = self.tx.send(()).await;
    }

    /// Shuts the compactor down.
    pub fn kill(&self) {
        self.tx.close();
    }
}

/// The manager for compacting blob files into larger files.
pub struct BlobCompactor {
    /// The compaction policy that controls the compactor.
    policy: Arc<dyn CompactionPolicy>,
    /// The counter for producing file IDs.
    file_key_counter: LockingCounter,
    /// The active read context.
    reader: ReadContext,
    /// The storage backend system.
    backend: StorageBackend,
    /// The storage service config.
    config: StorageServiceConfig,
    /// The compaction triggers.
    triggers: tachyonix::Receiver<()>,
    /// The transmitter for sending triggers,
    notify: tachyonix::Sender<()>,
}

impl BlobCompactor {
    #[instrument("compactor-spawn", skip_all)]
    /// Spawns a new blob compactor with a given policy.
    pub(crate) async fn spawn(
        policy: Box<dyn CompactionPolicy>,
        file_key_counter: LockingCounter,
        reader: ReadContext,
        config: StorageServiceConfig,
        backend: StorageBackend,
    ) -> CompactorController {
        let (tx, triggers) = tachyonix::channel(10);

        let actor = Self {
            policy: Arc::from(policy),
            file_key_counter,
            reader,
            config,
            backend,
            triggers,
            notify: tx.clone(),
        };

        tokio::spawn(actor.run());

        CompactorController { tx }
    }

    #[instrument("compactor", skip_all)]
    async fn run(mut self) {
        self.schedule_compact();

        info!("Compactor is ready");

        while let Ok(()) = self.triggers.recv().await {
            let start = Instant::now();
            match self.run_compaction().await {
                Ok(num_bytes) => {
                    let pretty_printed_size = humansize::format_size(
                        num_bytes.abs() as u64,
                        humansize::DECIMAL,
                    );
                    let pretty = if num_bytes < 0 {
                        format!("-{pretty_printed_size}")
                    } else {
                        format!("+{pretty_printed_size}")
                    };

                    info!(
                        reclaimed_bytes = num_bytes,
                        reclaimed_bytes_pretty = %pretty,
                        elapsed = ?start.elapsed(),
                        "Compaction completed"
                    );
                },
                Err(e) => {
                    error!(error = ?e, "Failed to run compaction due to error.");
                },
            }

            self.schedule_compact();

            // Drain any events that we might have collected while merging.
            while let Ok(()) = self.triggers.try_recv() {
                continue;
            }
        }

        info!("Compactor has shutdown");
    }

    /// Schedules a compaction cycle trigger.
    fn schedule_compact(&self) {
        let config = self.policy.get_config();

        let tx = self.notify.clone();
        tokio::spawn(async move {
            tokio::time::sleep(config.scan_interval).await;
            let _ = tx.send(()).await;
        });
    }

    async fn run_compaction(&mut self) -> io::Result<i64> {
        let file_key = match self.policy.get_safe_compact_checkpoint() {
            None => return Ok(0),
            Some(key) => key,
        };

        let active_writers = self.backend.get_active_writers();

        let active_writers_clone = active_writers.clone();
        let path = self.config.data_path();
        let files = tokio::task::spawn_blocking(move || {
            get_current_file_sizes_before(&path, file_key, active_writers_clone)
        })
        .await
        .expect("Spawn background thread")?;

        if files.len() < 2 {
            info!(
                num_files = files.len(),
                "Not enough files met the compaction criteria to warrant merging"
            );
            return Ok(0);
        }

        info!(
            num_files = files.len(),
            files = ?files,
            "{} files met compaction criteria",
            files.len()
        );

        let merge_buckets = self.get_merge_buckets(file_key, &active_writers);
        let plans = get_merge_plans(self.config.max_file_size, &merge_buckets);
        info!(
            num_plans = plans.len(),
            "Compaction plans have been created"
        );

        // Actually execute the compactions
        let mut bytes_written = 0;
        for (i, plan) in plans.into_iter().enumerate() {
            bytes_written += self.execute_plan(plan).await?;
            info!(plan = i + 1, "Completed compaction plan execution");
        }

        let mut bytes_reclaimed = 0;
        {
            let used_files = self.get_files_in_use(file_key);
            let dead_files = get_dead_files(&files, used_files);

            let cache = self.reader.cache();
            for (file, _) in dead_files.iter() {
                cache.forget(*file);
            }

            let path = self.config.data_path();
            bytes_reclaimed +=
                tokio::task::spawn_blocking(move || clean_dead_files(&path, dead_files))
                    .await
                    .expect("Spawn background thread")?;
        }

        Ok(bytes_reclaimed as i64 - bytes_written as i64)
    }

    /// Executes a merge plan.
    ///
    /// Returns the number of bytes written to the file.
    async fn execute_plan(&mut self, plan: MergePlan) -> io::Result<usize> {
        let file_key = FileKey(self.file_key_counter.inc());
        let data_path = self.config.data_path();

        let writer_path = get_data_file(&data_path, file_key);
        let writer = self.backend.open_writer(file_key, &writer_path).await?;
        self.policy.on_merge_file_creation(file_key);

        let write_context = WriteContext::new(self.reader.clone(), writer);

        self.copy_blobs_to_writer(write_context, plan.copy_blobs)
            .await
    }

    /// Copies a set of blob data to a file writer.
    async fn copy_blobs_to_writer(
        &mut self,
        mut writer: WriteContext,
        blobs: Vec<BlobMetadata>,
    ) -> io::Result<usize> {
        let config = self.policy.get_config();
        let prefetch = config.needs_data_for_delete;

        let (tx, mut rx) = tachyonix::channel(config.read_concurrency * 2);
        for chunk in blobs.chunks(config.read_concurrency) {
            let chunk = chunk.to_vec();

            let reader = self.reader.cache();
            let policy = self.policy.clone();

            tokio::spawn(copy_blob_data_chunk(
                prefetch,
                chunk,
                reader,
                policy,
                tx.clone(),
            ));
        }
        drop(tx);

        let mut bytes_written = 0;
        while let Ok((header, maybe_buffer)) = rx.recv().await {
            match maybe_buffer {
                None => writer.mark_delete(header.blob_id),
                Some(buffer) => {
                    let blob_id = header.blob_id;
                    let len = buffer.len();
                    let res = writer.write_header_and_data(header, buffer).await;

                    if let Err(e) = res {
                        error!(error = ?e, blob_id = blob_id, "Failed to write blob");
                        return Err(e);
                    }

                    bytes_written += len;
                    bytes_written += BlobHeader::SIZE;
                },
            }
        }

        writer.commit().await?;

        Ok(bytes_written)
    }

    /// Produces a set of buckets which can be used for compaction.
    ///
    /// This works by first filtering out any blobs which are part of a file that is
    /// newer than the given file key, then grouping the blobs by file_id and group_id.
    ///
    /// This lets us improve access patterns on the file cache and file in general by
    /// grouping data which is apart of the same 'group' of blobs.
    fn get_merge_buckets(
        &self,
        before: FileKey,
        active_writers: &HashSet<FileKey>,
    ) -> Vec<MergeBucket> {
        let mut buckets = HashMap::new();

        let reader = self.reader.blob_index().reader();
        let guard = reader.enter();

        for (id, info) in guard.iter().flatten() {
            let info = info.get_one().unwrap();

            if info.file_key >= before {
                continue;
            }

            if active_writers.contains(&info.file_key) {
                continue;
            }

            let bucket = buckets
                .entry((info.file_key, info.group_id))
                .or_insert_with(|| MergeBucket {
                    group_id: info.group_id,
                    file_key: info.file_key,
                    total_size_approx: 0,
                    blobs: Vec::with_capacity(1),
                });

            bucket.total_size_approx += info.total_length() as u64;
            bucket.blobs.push(BlobMetadata {
                id: *id,
                info: *info,
            });
        }

        // Collect and group the data.
        let mut buckets: Vec<MergeBucket> = buckets.into_values().collect();
        buckets.sort_by_key(|bucket| (bucket.group_id, bucket.total_size_approx));

        buckets
    }

    fn get_files_in_use(&self, before: FileKey) -> BTreeSet<FileKey> {
        self.reader
            .blob_index()
            .reader()
            .enter()
            .iter()
            .flatten()
            .filter_map(|(_, info)| {
                let info = info.get_one().unwrap();

                if info.file_key < before {
                    Some(info.file_key)
                } else {
                    None
                }
            })
            .collect()
    }
}

macro_rules! btreeset {
    ($( $val:expr $(,)? )+) => {{
        let mut tree = std::collections::BTreeSet::new();

        $(
            tree.insert($val);
        )*

        tree
    }};
}

/// Produces a set of merge plans from a given set of buckets.
///
/// This algorithm works off of the idea that it will incrementally combine larger
/// and larger files together.
///
/// It does this by first combining blobs with the same group_id, forming the a set
/// of plans with only one group ID as part of the merge.
///
/// Once this first step is done, we go over all of our plans and attempt to combine
/// multiple plans into one larger plan provided we don't go over the target size.
///
/// Not this approach does not provide the perfect size files right off the bat, it
/// gets our files reasonably close, but not exactly. It probably wants to become
/// a bit smarter later on.
fn get_merge_plans(target_size: u64, buckets: &[MergeBucket]) -> Vec<MergePlan> {
    if buckets.is_empty() {
        return Vec::new();
    }

    let mut plans = Vec::new();

    // Build a sparse set of plans which are done on the group_id.
    // These may not close to the target size, but it's a good step towards that goal.
    let mut current_plan = MergePlan::default();
    let mut current_group_id = None;
    for bucket in buckets {
        // If our groups don't match, or we're already at the target size.
        if Some(bucket.group_id) != current_group_id
            || (current_plan.estimated_size + bucket.total_size_approx) >= target_size
        {
            if !current_plan.group_ids.is_empty() {
                plans.push(current_plan);
            }

            current_plan = MergePlan {
                group_ids: btreeset! {bucket.group_id},
                file_keys: BTreeSet::new(),
                ..Default::default()
            };
            current_group_id = Some(bucket.group_id);
        }

        current_plan.estimated_size += bucket.total_size_approx;
        current_plan.copy_blobs.extend_from_slice(&bucket.blobs);
        current_plan.file_keys.insert(bucket.file_key);
    }

    if !current_plan.group_ids.is_empty() {
        plans.push(current_plan);
    }

    plans.sort_by_key(|p| p.group_ids.first().copied());

    let mut final_plans = Vec::new();
    let mut current_plan = MergePlan::default();
    // The first plan is always empty.
    for plan in plans {
        if plan.estimated_size >= target_size {
            if !current_plan.group_ids.is_empty() {
                final_plans.push(plan);
            }
            continue;
        }

        if current_plan.estimated_size + plan.estimated_size > target_size {
            if !current_plan.group_ids.is_empty() {
                final_plans.push(current_plan);
            }
            current_plan = MergePlan::default();
        }

        current_plan.group_ids.extend(plan.group_ids);
        current_plan.file_keys.extend(plan.file_keys);
        current_plan.copy_blobs.extend(plan.copy_blobs);
        current_plan.estimated_size += plan.estimated_size;
    }

    if !current_plan.group_ids.is_empty() {
        final_plans.push(current_plan);
    }

    final_plans
        .into_iter()
        .filter(|p| p.file_keys.len() > 1)
        .collect()
}

#[derive(Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
/// A planned merge operation of one or more files.
struct MergePlan {
    /// The estimated resulting file size.
    estimated_size: u64,
    /// The group IDs of the plan.
    group_ids: BTreeSet<u64>,
    /// The set of IDs of the plan.
    file_keys: BTreeSet<FileKey>,
    /// The blobs to copy into a new file.
    copy_blobs: Vec<BlobMetadata>,
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
/// A single merge operation bucket.
struct MergeBucket {
    /// The group ID of the bucket.
    group_id: u64,
    /// The file associated with the merge bucket.
    file_key: FileKey,
    /// The approximate size in bytes
    total_size_approx: u64,
    /// The blobs that are apart of that file.
    blobs: Vec<BlobMetadata>,
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
struct BlobMetadata {
    /// The ID of the blob.
    id: BlobId,
    /// The metadata info o the blob.
    info: BlobInfo,
}

#[instrument("compactor-file-finder")]
/// Creates a list of all files currently in the data directory
/// which are produced before the given `FileKey`.
fn get_current_file_sizes_before(
    path: &Path,
    newest_file_key: FileKey,
    active_writers: HashSet<FileKey>,
) -> io::Result<Vec<(FileKey, u64)>> {
    let dir = path.read_dir()?;

    let mut files = Vec::new();
    for entry in dir {
        let entry = entry?;
        let path = entry.path();
        let metadata = path.metadata()?;

        if metadata.is_dir() {
            warn!(path = %path.display(), "Ignoring unknown folder in data folder");
            continue;
        }

        let file_key = match crate::tools::parse_data_file_name(&path) {
            Some(file_key) => file_key,
            None => {
                warn!(path = %path.display(), "Ignoring unknown file");
                continue;
            },
        };

        if file_key < newest_file_key && !active_writers.contains(&file_key) {
            files.push((file_key, metadata.len()));
        }
    }

    Ok(files)
}

fn get_dead_files(
    files: &[(FileKey, u64)],
    files_in_use: BTreeSet<FileKey>,
) -> Vec<(FileKey, u64)> {
    let mut lookup = HashMap::from_iter(files.iter().copied());

    // Remove any files which exist in our index.
    for file_key in files_in_use {
        lookup.remove(&file_key);
    }

    lookup.into_iter().collect()
}

#[instrument("dead-file-gc", skip(dead_files))]
/// Removes any files which contain no data currently in the index.
fn clean_dead_files(
    data_path: &Path,
    dead_files: Vec<(FileKey, u64)>,
) -> io::Result<u64> {
    let mut num_bytes_cleaned = 0;
    let num_dead_files = dead_files.len();
    for (key, size) in dead_files {
        let path = get_data_file(data_path, key);
        match std::fs::remove_file(&path) {
            Err(e) => {
                warn!(path = %path.display(), error = ?e, "Failed to remove dead file due to error");
                continue;
            },
            Ok(()) => {
                num_bytes_cleaned += size;
                info!(path = %path.display(), "Removed dead file");
            },
        }
    }

    info!(
        num_dead_files = num_dead_files,
        reclaimed_bytes = num_bytes_cleaned,
        reclaimed_bytes_pretty = %humansize::format_size(num_bytes_cleaned, humansize::DECIMAL),
        "Dead files have been cleaned up"
    );

    Ok(num_bytes_cleaned)
}

/// Copies a chunk of blobs to the given writer.
async fn copy_blob_data_chunk(
    prefetch: bool,
    chunk: Vec<BlobMetadata>,
    reader: ReaderCache,
    policy: Arc<dyn CompactionPolicy>,
    tx: tachyonix::Sender<(BlobHeader, Option<ReadBuffer>)>,
) -> io::Result<()> {
    for blob in chunk {
        let buffer =
            fetch_and_check_against_policy(prefetch, blob, &reader, &policy).await?;

        let header = BlobHeader::new_with_merges(
            blob.id,
            blob.info.blob_length(),
            blob.info.group_id,
            blob.info.checksum,
            blob.info.merge_counter + 1,
        );

        tx.send((header, buffer)).await.map_err(|_| {
            io::Error::new(ErrorKind::Other, "Lost contact with receiver in blob copy")
        })?;
    }

    Ok(())
}

/// Reads the blob located at the position and returns it.
///
/// This method will check with the compaction policy if it can
/// safely delete the blob or not.
async fn fetch_and_check_against_policy(
    prefetch: bool,
    blob: BlobMetadata,
    reader: &ReaderCache,
    policy: &Arc<dyn CompactionPolicy>,
) -> io::Result<Option<ReadBuffer>> {
    let file_reader = reader.get_or_create(blob.info.file_key).await?;
    let pos = blob.info.start_pos;
    let len = blob.info.total_length;

    // We can potentially save some reads when handling deletes if we only
    // fetch the data that we need.
    let mut result = None;
    if prefetch {
        let buffer = file_reader.read_at(pos as usize, len as usize).await?;
        result = Some(buffer);
    }

    if policy.can_delete(blob.id, blob.info, result.as_ref()) {
        Ok(None)
    } else if let Some(result) = result {
        Ok(Some(result))
    } else {
        // Read the blob if we haven't already.
        let res = file_reader.read_at(pos as usize, len as usize).await?;
        Ok(Some(res))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algo_no_plans_with_1_file() {
        let buckets = vec![MergeBucket {
            group_id: 0,
            file_key: FileKey(1),
            total_size_approx: 50,
            blobs: vec![BlobMetadata {
                id: 1,
                info: Default::default(),
            }],
        }];
        let plans = get_merge_plans(100, &buckets);
        assert!(
            plans.is_empty(),
            "No plans should be made when only 1 file is available."
        );

        let buckets = vec![MergeBucket {
            group_id: 0,
            file_key: FileKey(1),
            total_size_approx: 50,
            blobs: vec![BlobMetadata {
                id: 1,
                info: Default::default(),
            }],
        }];
        let plans = get_merge_plans(45, &buckets);
        assert!(
            plans.is_empty(),
            "No plans should be made when only 1 file is available."
        );
    }

    #[test]
    fn test_algo_no_plans_with_2_file_too_big() {
        let buckets = vec![
            MergeBucket {
                group_id: 0,
                file_key: FileKey(1),
                total_size_approx: 50,
                blobs: vec![BlobMetadata {
                    id: 1,
                    info: Default::default(),
                }],
            },
            MergeBucket {
                group_id: 0,
                file_key: FileKey(2),
                total_size_approx: 50,
                blobs: vec![BlobMetadata {
                    id: 1,
                    info: Default::default(),
                }],
            },
        ];
        let plans = get_merge_plans(74, &buckets);
        assert!(
            plans.is_empty(),
            "No plans should be made when only 2 files are available but over sized."
        );
    }

    #[test]
    fn test_algo_plans_1_merge() {
        let buckets = vec![
            MergeBucket {
                group_id: 0,
                file_key: FileKey(1),
                total_size_approx: 50,
                blobs: vec![BlobMetadata {
                    id: 1,
                    info: Default::default(),
                }],
            },
            MergeBucket {
                group_id: 0,
                file_key: FileKey(2),
                total_size_approx: 50,
                blobs: vec![BlobMetadata {
                    id: 2,
                    info: Default::default(),
                }],
            },
        ];

        let plans = get_merge_plans(100, &buckets);
        assert_eq!(
            plans,
            vec![MergePlan {
                estimated_size: 100,
                group_ids: btreeset! {0},
                file_keys: btreeset! {FileKey(1), FileKey(2)},
                copy_blobs: vec![
                    BlobMetadata {
                        id: 1,
                        info: Default::default(),
                    },
                    BlobMetadata {
                        id: 2,
                        info: Default::default(),
                    }
                ],
            }]
        );
    }

    #[test]
    fn test_algo_plans_2_merges_to_group() {
        let buckets = vec![
            MergeBucket {
                group_id: 0,
                file_key: FileKey(1),
                total_size_approx: 50,
                blobs: vec![BlobMetadata {
                    id: 1,
                    info: Default::default(),
                }],
            },
            MergeBucket {
                group_id: 1,
                file_key: FileKey(1),
                total_size_approx: 50,
                blobs: vec![BlobMetadata {
                    id: 2,
                    info: Default::default(),
                }],
            },
            MergeBucket {
                group_id: 0,
                file_key: FileKey(2),
                total_size_approx: 50,
                blobs: vec![BlobMetadata {
                    id: 3,
                    info: Default::default(),
                }],
            },
            MergeBucket {
                group_id: 1,
                file_key: FileKey(2),
                total_size_approx: 50,
                blobs: vec![BlobMetadata {
                    id: 4,
                    info: Default::default(),
                }],
            },
        ];

        let plans = get_merge_plans(100, &buckets);
        assert_eq!(
            plans,
            vec![
                MergePlan {
                    estimated_size: 100,
                    group_ids: btreeset! {0},
                    file_keys: btreeset! {FileKey(1), FileKey(2)},
                    copy_blobs: vec![
                        BlobMetadata {
                            id: 1,
                            info: Default::default(),
                        },
                        BlobMetadata {
                            id: 3,
                            info: Default::default(),
                        }
                    ],
                },
                MergePlan {
                    estimated_size: 100,
                    group_ids: btreeset! {1},
                    file_keys: btreeset! {FileKey(1), FileKey(2)},
                    copy_blobs: vec![
                        BlobMetadata {
                            id: 2,
                            info: Default::default(),
                        },
                        BlobMetadata {
                            id: 4,
                            info: Default::default(),
                        }
                    ],
                }
            ]
        );
    }
}
