// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use matrixobject_client::{ClientError, S3Client, TcpTransport, Transport};
pub use matrixobject_client::AppendResolutions;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("invalid object key: {0}")]
    InvalidKey(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http error: {0}")]
    Http(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A durable compare-and-set precondition failed: the object either did not hold the
    /// expected bytes (lost a race / stale owner) or the backend does not support a fenced
    /// conditional put. This is the single-writer FENCE signal — callers MUST treat it as a
    /// hard abort, never a retryable transient error.
    #[error("object store condition failed for {key}: {detail}")]
    ConditionFailed { key: String, detail: String },
    /// A write was sent and neither its success nor its absence could be established.
    ///
    /// The write may or may not be in the object. A caller must NOT repeat it blindly -- for an
    /// append that would store the bytes twice -- and must settle it by reading the object
    /// first.
    #[error("object store outcome unknown for {key}: {detail}")]
    OutcomeUnknown { key: String, detail: String },
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct AppendBlobReceipt {
    pub key: String,
    pub start_offset: u64,
    pub end_offset: u64,
    pub bytes_written: u64,
    pub object_length: u64,
    pub physical_slab_count: usize,
    pub first_physical_offset: Option<u64>,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError>;
    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        let _ = (key, bytes);
        Err(ObjectStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this object store does not implement append_blob; replacing an object is not an append",
        )))
    }

    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        for (key, bytes) in objects {
            self.put(key, bytes.clone()).await?;
        }
        Ok(())
    }
    /// Durable compare-and-set: atomically replace the object at `key` with `new` iff its
    /// current contents exactly equal `expected` (`None` meaning "the object must not exist").
    /// This is the substrate for a single-writer fence/lease: a stale owner that has been
    /// superseded observes a value it does not expect and is rejected with
    /// [`ObjectStoreError::ConditionFailed`]. The default implementation is FAIL-CLOSED — it
    /// refuses rather than performing an unfenced read-modify-write — so a backend without a
    /// real atomic conditional put can never silently degrade the fence to a racy no-op.
    /// Overridden with a real cross-process atomic CAS by [`FileObjectStore`].
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new: Bytes,
    ) -> Result<(), ObjectStoreError> {
        let _ = (expected, new);
        Err(ObjectStoreError::ConditionFailed {
            key: key.to_string(),
            detail: "compare_and_swap unsupported by this object store backend".to_string(),
        })
    }
    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError>;
    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, ObjectStoreError> {
        let bytes = self.get(key).await?;
        let start = usize::try_from(offset)
            .map_err(|_| ObjectStoreError::InvalidKey(format!("{key}: range offset too large")))?;
        let len = usize::try_from(length)
            .map_err(|_| ObjectStoreError::InvalidKey(format!("{key}: range length too large")))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| ObjectStoreError::InvalidKey(format!("{key}: range overflow")))?;
        if end > bytes.len() {
            return Err(ObjectStoreError::InvalidKey(format!(
                "{key}: range {offset}..{} exceeds object length {}",
                offset.saturating_add(length),
                bytes.len()
            )));
        }
        Ok(bytes.slice(start..end))
    }
    /// The bytes of `key` from `offset` to its end, or `None` when the object is SHORTER than
    /// `offset`. At exactly the object's length the answer is `Some` of nothing.
    ///
    /// For a reader that follows an append-only object: it already holds everything before
    /// `offset` and wants only what has been added since, so asking for the whole object --
    /// which grows without bound -- every time it checks is a cost proportional to the log, paid
    /// on every poll. `None` is how a store says the object is not the one the caller was
    /// following any more (it was replaced by something shorter), which the caller must treat as
    /// a reason to start over rather than as an error.
    ///
    /// The default reads the whole object and slices it, which is correct for every store and
    /// leaves the saving to the stores that can range-read.
    async fn get_range_from(
        &self,
        key: &str,
        offset: u64,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let bytes = self.get(key).await?;
        let start = usize::try_from(offset)
            .map_err(|_| ObjectStoreError::InvalidKey(format!("{key}: range offset too large")))?;
        if start > bytes.len() {
            return Ok(None);
        }
        Ok(Some(bytes.slice(start..)))
    }
    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        let mut objects = Vec::with_capacity(keys.len());
        for key in keys {
            objects.push((key.clone(), self.get(key).await?));
        }
        Ok(objects)
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError>;
    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<String>, ObjectStoreError> {
        Ok(self
            .list(prefix)
            .await?
            .into_iter()
            .filter(|key| key.as_str() > after)
            .collect())
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;
    fn uri(&self, key: &str) -> String;
}

/// Networked object store speaking S3 to a MatrixObject door.
///
/// A thin adapter. Everything about talking to the door lives once, in
/// `matrixobject_client::S3Client`: the request framing, the append with its offset condition,
/// the settlement of an append whose outcome is unknown by its attempt id (never by comparing
/// bytes), the length cache, the keep-alive sockets and the wire accounting. This type maps
/// that client's answers onto [`ObjectStore`] and nothing else.
///
/// The door is a [`Transport`]: a socket to a gateway (`new`), or the gateway's own request
/// handling served in this process (`in_process`). Both run the same client code, so the two
/// ways of reaching a cluster cannot disagree about a request, only about how it travels.
#[derive(Debug, Clone)]
pub struct MatrixObjectHttpStore {
    client: S3Client<Arc<dyn Transport>>,
    uri: String,
    /// One lock per object key, held around an append across its awaits.
    ///
    /// The client serialises appends of one object itself, but it does so by blocking a thread.
    /// Tasks of this process that append to the same object wait here instead, yielding, so a
    /// burst of appends to one log cannot occupy every blocking thread.
    append_locks: Arc<Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

/// Bucket a URI that names none will use.
///
/// `matrixobject://host:port` carries no bucket and every caller so far runs one store per
/// endpoint; `matrixobject://host:port/name` names one explicitly.
pub const DEFAULT_HTTP_BUCKET: &str = "temporalstore";

impl From<ClientError> for ObjectStoreError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::NotFound(key) => ObjectStoreError::NotFound(key),
            ClientError::InvalidRange(detail)
            | ClientError::InvalidKey(detail)
            | ClientError::InvalidBucket(detail) => ObjectStoreError::InvalidKey(detail),
            ClientError::ConditionFailed { key, detail } => {
                ObjectStoreError::ConditionFailed { key, detail }
            }
            ClientError::OutcomeUnknown { key, detail }
            | ClientError::ConditionalWriteUnknown { key, detail } => {
                ObjectStoreError::OutcomeUnknown { key, detail }
            }
            ClientError::Http(detail) | ClientError::Transport(detail) => {
                ObjectStoreError::Http(detail)
            }
            // `ClientError` is non-exhaustive: an error this adapter does not know is a store failure.
            other => ObjectStoreError::Http(other.to_string()),
        }
    }
}

impl MatrixObjectHttpStore {
    pub fn new(uri: &str) -> Result<Self, ObjectStoreError> {
        let trimmed = uri.trim();
        let rest = trimmed
            .strip_prefix("matrixobject://")
            .ok_or_else(|| ObjectStoreError::InvalidKey(trimmed.to_string()))?;
        let (authority, bucket) = match rest.split_once('/') {
            Some((authority, bucket)) => {
                let bucket = bucket.trim_matches('/');
                (
                    authority,
                    if bucket.is_empty() {
                        DEFAULT_HTTP_BUCKET
                    } else {
                        bucket
                    },
                )
            }
            None => (rest, DEFAULT_HTTP_BUCKET),
        };
        let authority = authority.trim();
        if authority.is_empty() {
            return Err(ObjectStoreError::InvalidKey(trimmed.to_string()));
        }
        Ok(Self::over(
            Arc::new(TcpTransport::new(authority)),
            bucket,
            trimmed.trim_end_matches('/').to_string(),
        ))
    }

    /// A store whose requests are answered by `door`, typically the gateway's request handling
    /// served in this process.
    pub fn in_process(door: Arc<dyn Transport>, bucket: &str) -> Self {
        let bucket = if bucket.trim().is_empty() {
            DEFAULT_HTTP_BUCKET
        } else {
            bucket.trim()
        };
        Self::over(door, bucket, format!("matrixobject://in-process/{bucket}"))
    }

    fn over(door: Arc<dyn Transport>, bucket: &str, uri: String) -> Self {
        Self {
            client: S3Client::new(door, bucket),
            uri,
            append_locks: Arc::default(),
        }
    }

    /// Which bucket this endpoint's objects live in.
    pub fn bucket(&self) -> &str {
        self.client.bucket()
    }

    /// Synchronous single-object GET for callers outside an async runtime -- the networked
    /// analogue of a plain `std::fs::read`. A sync lazy slab source resolves a checkpoint slab
    /// by address without bridging into an executor, mirroring how the shared-filesystem lazy
    /// source reads the file directly.
    pub fn get_blocking(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        Ok(Bytes::from(self.client.get(key)?))
    }

    /// How many HTTP requests this store has issued since it was built.
    pub fn requests_issued(&self) -> u64 {
        self.client.requests_issued()
    }

    /// Bytes written to the socket, request heads included. Zero for a door in this process.
    pub fn wire_bytes_sent(&self) -> u64 {
        self.client.transport().wire_stats().bytes_sent
    }

    /// Bytes read from the socket, response heads included.
    pub fn wire_bytes_received(&self) -> u64 {
        self.client.transport().wire_stats().bytes_received
    }

    /// How many TCP connections this store has opened. With reuse this is far below
    /// `requests_issued`.
    pub fn connections_opened(&self) -> u64 {
        self.client.transport().wire_stats().connections_opened
    }

    /// How many landed, absent and ambiguous answers the door has given to a resolve of an
    /// append whose outcome was unknown. A caller that must observe a particular answer reads it here.
    pub fn append_resolutions(&self) -> AppendResolutions {
        self.client.append_resolutions()
    }

    /// The lock that serialises appends to `key` among the tasks of this process.
    fn append_lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.append_locks.lock().expect("matrixobject append locks poisoned");
        // Bound the table: drop locks nobody holds or waits on once it has grown.
        if locks.len() >= 1024 {
            locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
        locks.entry(key.to_string()).or_default().clone()
    }
}

#[async_trait]
impl ObjectStore for MatrixObjectHttpStore {
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new: Bytes,
    ) -> Result<(), ObjectStoreError> {
        let lock = self.append_lock(key);
        let _serialised = lock.lock().await;
        let client = self.client.clone();
        let owned_key = key.to_string();
        tokio::task::spawn_blocking(move || {
            client.compare_and_swap(&owned_key, expected.as_deref(), new.as_ref())
        })
        .await
        .map_err(|error| ObjectStoreError::OutcomeUnknown {
            key: key.to_string(),
            detail: format!("the conditional write task did not finish: {error}"),
        })??;
        Ok(())
    }

    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        Ok(self.client.put(key, bytes.as_ref())?)
    }

    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        // ONE APPEND PER OBJECT AT A TIME WITHIN THIS PROCESS: see `append_locks`.
        let lock = self.append_lock(key);
        let _serialised = lock.lock().await;
        // The client's append is blocking (it may settle an unknown outcome with further
        // requests), so it runs on a blocking thread and never stalls the executor.
        let client = self.client.clone();
        let owned_key = key.to_string();
        let receipt = tokio::task::spawn_blocking(move || client.append(&owned_key, bytes.as_ref()))
            .await
            .map_err(|error| {
                ObjectStoreError::Http(format!("{key}: the append task did not finish: {error}"))
            })??;
        Ok(AppendBlobReceipt {
            key: key.to_string(),
            start_offset: receipt.start_offset,
            end_offset: receipt.end_offset,
            bytes_written: receipt.bytes_written,
            object_length: receipt.object_length,
            // NOT KNOWABLE OVER THE WIRE, and reported as absent rather than guessed --
            // exactly as `FileObjectStore` reports them.
            physical_slab_count: 0,
            first_physical_offset: None,
        })
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        self.get_blocking(key)
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, ObjectStoreError> {
        Ok(Bytes::from(self.client.get_range(key, offset, length)?))
    }

    async fn get_range_from(
        &self,
        key: &str,
        offset: u64,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        Ok(self.client.get_range_from(key, offset)?.map(Bytes::from))
    }

    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        let mut objects = Vec::with_capacity(keys.len());
        for key in keys {
            objects.push((key.clone(), self.get(key).await?));
        }
        Ok(objects)
    }

    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        for (key, bytes) in objects {
            self.put(key, bytes.clone()).await?;
        }
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        Ok(self.client.list(prefix, None)?)
    }

    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<String>, ObjectStoreError> {
        // `start-after` is served by the door, so the filtering happens there instead of every
        // key crossing the wire to be dropped here.
        Ok(self.client.list(prefix, Some(after))?)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        Ok(self.client.delete(key)?)
    }

    fn uri(&self, key: &str) -> String {
        format!("{}/{}", self.uri, key)
    }
}

/// Where an append-mode write ended, as the file itself reports it.
///
/// On unix the position after a write to a file opened for append is the end of that write,
/// set under the same inode lock as the write, so it stays right while other appenders run.
#[cfg(unix)]
fn landed_end_offset(file: &mut std::fs::File) -> std::io::Result<u64> {
    use std::io::Seek;
    file.stream_position()
}

/// Elsewhere an append-mode handle's position is not defined to follow its writes, so the
/// length is the best available answer. It can be later than this write's end if another
/// appender got in first, which the unix path above does not allow.
#[cfg(not(unix))]
fn landed_end_offset(file: &mut std::fs::File) -> std::io::Result<u64> {
    file.metadata().map(|metadata| metadata.len())
}

#[derive(Debug, Clone)]
pub struct FileObjectStore {
    root: PathBuf,
    uri_scheme: String,
    created_dirs: Arc<Mutex<HashSet<PathBuf>>>,
}

impl FileObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            uri_scheme: "file".to_string(),
            created_dirs: Arc::default(),
        }
    }

    pub fn with_uri_scheme(root: impl Into<PathBuf>, uri_scheme: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            uri_scheme: uri_scheme.into(),
            created_dirs: Arc::default(),
        }
    }

    fn resolve(&self, key: &str) -> Result<PathBuf, ObjectStoreError> {
        if key.contains("..") || key.starts_with('/') || key.starts_with('\\') {
            return Err(ObjectStoreError::InvalidKey(key.to_string()));
        }
        Ok(self
            .root
            .join(key.replace('/', std::path::MAIN_SEPARATOR_STR)))
    }

    /// Filesystem path where `key` is stored. Exposed so a streaming reader (e.g.
    /// a streamed HTTP `GET /blob/<key>`) can `stat` + read the object in chunks
    /// straight to the socket instead of loading the whole object into memory via
    /// [`ObjectStore::get`]. Applies the same key-safety guard as `resolve`.
    pub fn object_path(&self, key: &str) -> Result<PathBuf, ObjectStoreError> {
        self.resolve(key)
    }
}

#[async_trait]
impl ObjectStore for FileObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        let path = self.resolve(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        let path = self.resolve(key)?;
        if let Some(parent) = path.parent() {
            // REMEMBERED ONLY ONCE IT EXISTS. This used to record the directory first and
            // create it after, so a second appender arriving in between saw "already created",
            // skipped the create, and opened a file in a directory that was not there yet.
            // `create_dir_all` is idempotent, so two callers both creating it costs nothing.
            let known = self
                .created_dirs
                .lock()
                .expect("object-store dir cache poisoned")
                .contains(parent);
            if !known {
                tokio::fs::create_dir_all(parent).await?;
                self.created_dirs
                    .lock()
                    .expect("object-store dir cache poisoned")
                    .insert(parent.to_path_buf());
            }
        }
        let bytes_written = bytes.len() as u64;
        // THE OFFSET IS WHERE THE BYTES LANDED, read after the write and never before it.
        //
        // This used to `stat` the file for its length, then open and append, and took the stat
        // as the start. Two appenders that both stat'd before either wrote claimed the same
        // start, while append mode placed each at the real end: receipts that overlapped each
        // other and named ranges holding someone else's bytes.
        //
        // An append-mode write moves the file position to the end of that write as part of the
        // write itself, so the position afterwards is this write's end and the start is that
        // minus its length. The write is one blocking call on a plain `std::fs::File` -- a
        // `tokio::fs::File` splits a large buffer into several writes, and a concurrent append
        // landing between them would put this one's bytes in two places.
        let end_offset = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            file.write_all(&bytes)?;
            file.flush()?;
            landed_end_offset(&mut file)
        })
        .await
        .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))??;
        let start_offset = end_offset.saturating_sub(bytes_written);
        Ok(AppendBlobReceipt {
            key: key.to_string(),
            start_offset,
            end_offset,
            bytes_written,
            object_length: end_offset,
            physical_slab_count: 0,
            first_physical_offset: None,
        })
    }

    /// Cross-process atomic compare-and-set. Mutual exclusion is provided by an atomic
    /// `mkdir` of a per-key lock directory (POSIX `mkdir` fails if the directory already
    /// exists, and this atomicity holds over a shared filesystem too), so a concurrent
    /// writer on another host cannot interleave its read-compare-write with ours. Inside the
    /// lock we read the current object, reject with [`ObjectStoreError::ConditionFailed`] if
    /// it does not exactly equal `expected`, then durably (`sync_all`) install `new`.
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new: Bytes,
    ) -> Result<(), ObjectStoreError> {
        let path = self.resolve(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let lock_path = {
            let mut p = path.clone().into_os_string();
            p.push(".caslock");
            PathBuf::from(p)
        };
        let _guard = CasLockGuard::acquire(lock_path).await?;
        let current = match tokio::fs::read(&path).await {
            Ok(bytes) => Some(Bytes::from(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(ObjectStoreError::Io(err)),
        };
        if current.as_deref() != expected.as_deref() {
            return Err(ObjectStoreError::ConditionFailed {
                key: key.to_string(),
                detail: "current object contents do not match expected fence value".to_string(),
            });
        }
        let mut file = tokio::fs::File::create(&path).await?;
        file.write_all(&new).await?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let path = self.resolve(key)?;
        match tokio::fs::read(path).await {
            Ok(bytes) => Ok(Bytes::from(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(ObjectStoreError::NotFound(key.to_string()))
            }
            Err(err) => Err(ObjectStoreError::Io(err)),
        }
    }

    async fn get_range_from(
        &self,
        key: &str,
        offset: u64,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let path = self.resolve(key)?;
        let mut file = match tokio::fs::File::open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjectStoreError::NotFound(key.to_string()))
            }
            Err(err) => return Err(ObjectStoreError::Io(err)),
        };
        let length = file.metadata().await?.len();
        if offset > length {
            return Ok(None);
        }
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut tail = Vec::with_capacity((length - offset) as usize);
        file.read_to_end(&mut tail).await?;
        Ok(Some(Bytes::from(tail)))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let mut out = Vec::new();
        let root = self.root.clone();
        if !root.exists() {
            return Ok(out);
        }
        collect_files(&root, &root, &mut out).await?;
        out.retain(|key| key.starts_with(prefix));
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let path = self.resolve(key)?;
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(ObjectStoreError::Io(err)),
        }
    }

    fn uri(&self, key: &str) -> String {
        format!("{}://{}", self.uri_scheme, key)
    }
}

/// RAII cross-process lock backed by an atomic `mkdir`. Acquisition spins on `create_dir`
/// (which fails atomically with `AlreadyExists` while another holder owns it) up to a bounded
/// deadline; the directory is removed on drop so the lock is released even on the error path.
struct CasLockGuard {
    lock_path: PathBuf,
}

impl CasLockGuard {
    async fn acquire(lock_path: PathBuf) -> Result<Self, ObjectStoreError> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match tokio::fs::create_dir(&lock_path).await {
                Ok(()) => return Ok(Self { lock_path }),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if std::time::Instant::now() >= deadline {
                        return Err(ObjectStoreError::ConditionFailed {
                            key: lock_path.to_string_lossy().to_string(),
                            detail: "timed out acquiring compare-and-swap lock".to_string(),
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Err(err) => return Err(ObjectStoreError::Io(err)),
            }
        }
    }
}

impl Drop for CasLockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.lock_path);
    }
}

async fn collect_files(
    root: &Path,
    dir: &Path,
    out: &mut Vec<String>,
) -> Result<(), ObjectStoreError> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&current).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let ty = entry.file_type().await?;
            if ty.is_dir() {
                stack.push(path);
            } else if ty.is_file() {
                let rel = path.strip_prefix(root).map_err(|_| {
                    ObjectStoreError::InvalidKey(path.to_string_lossy().to_string())
                })?;
                out.push(rel.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    Ok(())
}

// ---- How a node reaches MatrixObject over the network ----------------------------------

/// The two ways a TemporalStore node can reach a MatrixObject cluster.
///
/// `Gateway` speaks S3 to a gateway (`MatrixObjectHttpStore`). `Direct` talks to the metadata
/// group and the chunk servers itself, with no gateway in between. Both are meant to give the
/// same TemporalStore behaviour and the same durable bytes; the mode only chooses the path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatrixObjectMode {
    Gateway,
    Direct,
}

/// A networked MatrixObject configuration that cannot be used as written.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MatrixObjectModeError {
    #[error("TS_MATRIXOBJECT_MODE={0:?} is not a mode; use \"gateway\" or \"direct\"")]
    UnknownMode(String),
    #[error("gateway mode needs TS_SHARED_STORE_URI=matrixobject://host:port")]
    GatewayNeedsUri,
    #[error("direct mode needs TS_MATRIXOBJECT_META, the metadata group's endpoints")]
    DirectNeedsMeta,
}

/// Pick the mode from the three settings that name it, or refuse. Pure, so every outcome is
/// testable without touching the process environment. An empty or blank value is unset.
///
/// * `mode`: `TS_MATRIXOBJECT_MODE`.
/// * `gateway_uri`: `TS_SHARED_STORE_URI`, which counts only when it names `matrixobject://`.
/// * `direct_meta`: `TS_MATRIXOBJECT_META`.
///
/// Direct is the default: with no mode, a metadata group decides for direct whether or not a
/// gateway is also configured, and a gateway is used only when it is the only thing configured
/// (a node cannot reach a cluster directly without knowing its metadata group). An explicit mode
/// wins and needs only its own setting.
///
/// `Ok(None)` means no networked MatrixObject is configured at all.
pub fn resolve_matrixobject_mode(
    mode: Option<&str>,
    gateway_uri: Option<&str>,
    direct_meta: Option<&str>,
) -> Result<Option<MatrixObjectMode>, MatrixObjectModeError> {
    fn set(value: Option<&str>) -> Option<&str> {
        value.map(str::trim).filter(|value| !value.is_empty())
    }
    let gateway = set(gateway_uri).filter(|uri| uri.starts_with("matrixobject://"));
    let direct = set(direct_meta);
    match set(mode).map(str::to_ascii_lowercase).as_deref() {
        Some("gateway") => match gateway {
            None => Err(MatrixObjectModeError::GatewayNeedsUri),
            Some(_) => Ok(Some(MatrixObjectMode::Gateway)),
        },
        Some("direct") => match direct {
            None => Err(MatrixObjectModeError::DirectNeedsMeta),
            Some(_) => Ok(Some(MatrixObjectMode::Direct)),
        },
        Some(other) => Err(MatrixObjectModeError::UnknownMode(other.to_string())),
        None => match (direct, gateway) {
            (Some(_), _) => Ok(Some(MatrixObjectMode::Direct)),
            (None, Some(_)) => Ok(Some(MatrixObjectMode::Gateway)),
            (None, None) => Ok(None),
        },
    }
}

/// A synchronous whole-object read, for the callers that cannot be async (the lazy slab
/// source is consulted from inside a block store's read path).
pub trait BlockingObjectRead: Send + Sync {
    fn get_blocking(&self, key: &str) -> Result<Bytes, ObjectStoreError>;
}

impl BlockingObjectRead for MatrixObjectHttpStore {
    fn get_blocking(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        MatrixObjectHttpStore::get_blocking(self, key)
    }
}

/// The networked MatrixObject store a node holds, whichever way it reaches the cluster.
///
/// Only `Gateway` exists today; `Direct` joins it when the SDK client does. The enum is here
/// first so everything that holds a networked store holds this, and adding the second path is
/// then a change to this file and not to every caller.
///
/// Every `ObjectStore` method is forwarded, the defaulted ones included: a wrapper that omits
/// `append_blob` or `compare_and_swap` compiles and then refuses at run time. The guard test
/// in `shared_store` reads the trait and checks this impl for each method.
#[derive(Debug, Clone)]
pub enum NetworkedMatrixObject {
    Gateway(MatrixObjectHttpStore),
    /// The same store, its requests answered in process by the cluster client.
    Direct(MatrixObjectHttpStore),
}

impl NetworkedMatrixObject {
    /// Connect in the given mode. `target` is the gateway URI for `Gateway`.
    pub fn connect(mode: MatrixObjectMode, target: &str) -> Result<Self, ObjectStoreError> {
        match mode {
            MatrixObjectMode::Gateway => Ok(Self::Gateway(MatrixObjectHttpStore::new(target)?)),
            MatrixObjectMode::Direct => Err(ObjectStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "direct MatrixObject mode is connected by `matrixobject_direct::DirectConfig` in temporalstore-rust, not by this crate",
            ))),
        }
    }

    /// The direct way in: a store whose requests `door` answers in this process.
    pub fn direct(door: Arc<dyn Transport>, bucket: &str) -> Self {
        Self::Direct(MatrixObjectHttpStore::in_process(door, bucket))
    }

    pub fn mode(&self) -> MatrixObjectMode {
        match self {
            Self::Gateway(_) => MatrixObjectMode::Gateway,
            Self::Direct(_) => MatrixObjectMode::Direct,
        }
    }

    pub fn bucket(&self) -> &str {
        match self {
            Self::Gateway(store) => store.bucket(),
            Self::Direct(store) => store.bucket(),
        }
    }

    pub fn requests_issued(&self) -> u64 {
        match self {
            Self::Gateway(store) => store.requests_issued(),
            Self::Direct(store) => store.requests_issued(),
        }
    }

    pub fn wire_bytes_sent(&self) -> u64 {
        match self {
            Self::Gateway(store) => store.wire_bytes_sent(),
            Self::Direct(store) => store.wire_bytes_sent(),
        }
    }

    pub fn wire_bytes_received(&self) -> u64 {
        match self {
            Self::Gateway(store) => store.wire_bytes_received(),
            Self::Direct(store) => store.wire_bytes_received(),
        }
    }

    pub fn connections_opened(&self) -> u64 {
        match self {
            Self::Gateway(store) => store.connections_opened(),
            Self::Direct(store) => store.connections_opened(),
        }
    }

    pub fn append_resolutions(&self) -> AppendResolutions {
        match self {
            Self::Gateway(store) => store.append_resolutions(),
            Self::Direct(store) => store.append_resolutions(),
        }
    }
}

impl BlockingObjectRead for NetworkedMatrixObject {
    fn get_blocking(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::Gateway(store) => BlockingObjectRead::get_blocking(store, key),
            Self::Direct(store) => BlockingObjectRead::get_blocking(store, key),
        }
    }
}

#[async_trait]
impl ObjectStore for NetworkedMatrixObject {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.put(key, bytes).await,
            Self::Direct(store) => store.put(key, bytes).await,
        }
    }

    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.append_blob(key, bytes).await,
            Self::Direct(store) => store.append_blob(key, bytes).await,
        }
    }

    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.put_many(objects).await,
            Self::Direct(store) => store.put_many(objects).await,
        }
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new: Bytes,
    ) -> Result<(), ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.compare_and_swap(key, expected, new).await,
            Self::Direct(store) => store.compare_and_swap(key, expected, new).await,
        }
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.get(key).await,
            Self::Direct(store) => store.get(key).await,
        }
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.get_range(key, offset, length).await,
            Self::Direct(store) => store.get_range(key, offset, length).await,
        }
    }

    async fn get_range_from(
        &self,
        key: &str,
        offset: u64,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.get_range_from(key, offset).await,
            Self::Direct(store) => store.get_range_from(key, offset).await,
        }
    }

    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.get_many(keys).await,
            Self::Direct(store) => store.get_many(keys).await,
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.list(prefix).await,
            Self::Direct(store) => store.list(prefix).await,
        }
    }

    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<String>, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.list_after(prefix, after).await,
            Self::Direct(store) => store.list_after(prefix, after).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.delete(key).await,
            Self::Direct(store) => store.delete(key).await,
        }
    }

    fn uri(&self, key: &str) -> String {
        match self {
            Self::Gateway(store) => store.uri(key),
            Self::Direct(store) => store.uri(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_put_only_store_refuses_append_without_replacing_bytes() {
        struct PutOnly(FileObjectStore);
        #[async_trait]
        impl ObjectStore for PutOnly {
            async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
                self.0.put(key, bytes).await
            }
            async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
                self.0.get(key).await
            }
            async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
                self.0.list(prefix).await
            }
            async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
                self.0.delete(key).await
            }
            fn uri(&self, key: &str) -> String {
                self.0.uri(key)
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let store = PutOnly(FileObjectStore::new(dir.path()));
        store
            .put("wal", Bytes::from_static(b"first"))
            .await
            .unwrap();
        let error = store
            .append_blob("wal", Bytes::from_static(b"second"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, ObjectStoreError::Io(ref error) if error.kind() == std::io::ErrorKind::Unsupported)
        );
        assert_eq!(
            store.get("wal").await.unwrap(),
            Bytes::from_static(b"first")
        );
    }

    #[tokio::test]
    async fn file_object_store_append_blob_preserves_existing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileObjectStore::new(dir.path());

        store
            .append_blob("wal/blob.pb", Bytes::from_static(b"first"))
            .await
            .unwrap();
        let receipt = store
            .append_blob("wal/blob.pb", Bytes::from_static(b"second"))
            .await
            .unwrap();

        assert_eq!(
            store.get("wal/blob.pb").await.unwrap(),
            Bytes::from_static(b"firstsecond")
        );
        assert_eq!(receipt.start_offset, 5);
        assert_eq!(receipt.end_offset, 11);
        assert_eq!(receipt.object_length, 11);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_report_the_offsets_their_bytes_landed_at() {
        // THE RECEIPT HAS TO NAME WHERE THE BYTES LANDED, not where the file ended when the
        // append began. The offset used to be read with a `stat` before the file was opened,
        // so two appenders that stat'd the same length both claimed it, while the append-mode
        // write put each at the real end: receipts that overlapped each other and described
        // bytes that were not there. Every receipt here is checked against the object itself.
        const WRITERS: usize = 8;
        const APPENDS: usize = 60;
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FileObjectStore::new(dir.path()));

        let mut tasks = Vec::new();
        for writer in 0..WRITERS {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                let mut receipts = Vec::new();
                for round in 0..APPENDS {
                    // A byte value and a length unique to the writer, so a range holding
                    // anyone else's bytes cannot pass for this one.
                    let payload = vec![b'a' + writer as u8; 7 + writer * 3 + round % 5];
                    let receipt = store
                        .append_blob("wal/shared", Bytes::from(payload.clone()))
                        .await
                        .unwrap();
                    receipts.push((receipt, payload));
                }
                receipts
            }));
        }
        let mut receipts = Vec::new();
        for task in tasks {
            receipts.extend(task.await.unwrap());
        }

        let object = store.get("wal/shared").await.unwrap();
        let total: u64 = receipts.iter().map(|(_, payload)| payload.len() as u64).sum();
        assert_eq!(object.len() as u64, total, "every appended byte is in the object");

        receipts.sort_by_key(|(receipt, _)| receipt.start_offset);
        let mut expected_start = 0u64;
        for (receipt, payload) in &receipts {
            assert_eq!(
                receipt.start_offset, expected_start,
                "receipts must tile the object: a hole or an overlap means an offset is wrong"
            );
            assert_eq!(receipt.end_offset, expected_start + payload.len() as u64);
            assert_eq!(receipt.bytes_written, payload.len() as u64);
            let landed = &object[receipt.start_offset as usize..receipt.end_offset as usize];
            assert_eq!(landed, payload.as_slice(), "the range a receipt names holds its bytes");
            expected_start = receipt.end_offset;
        }
        assert_eq!(expected_start, total);
    }

    #[tokio::test]
    async fn file_object_store_put_is_reopen_readable_after_durable_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileObjectStore::new(dir.path().join("objects"));

        store
            .put("shards/1/wal/0001.json", Bytes::from_static(b"durable"))
            .await
            .unwrap();

        let reopened = tokio::fs::read(dir.path().join("objects/shards/1/wal/0001.json"))
            .await
            .unwrap();
        assert_eq!(reopened, b"durable");
        assert_eq!(
            store.get("shards/1/wal/0001.json").await.unwrap(),
            Bytes::from_static(b"durable")
        );
    }

    // ---- An append whose outcome is unknown is settled by reading, never repeated blind ----
    //
    // A scripted one-object S3 server. It commits a PUT and THEN misbehaves (drops the socket,
    // answers 503), which is the case a real gateway produces when a consensus deadline passes
    // after the manifest was committed.

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Fault {
        None,
        /// Commit the append, then close the socket without answering.
        CommitThenDrop,
        /// Commit the append, then answer 503.
        CommitThen503,
        /// Answer 503 and commit nothing.
        Refuse503,
    }

    #[derive(Debug)]
    struct ScriptedState {
        data: Vec<u8>,
        existing: bool,
        puts: usize,
        fault_on_first_put: Fault,
        /// Every resolve answers 503 (the gateway cannot decide).
        reads_fail: bool,
        /// The attempt ids of the appends this object committed.
        committed_attempts: Vec<String>,
        /// Every attempt id a PUT arrived with, committed or not.
        put_attempts: Vec<String>,
    }

    /// What the scripted one-object door answers, as raw HTTP bytes, or `None` to drop the
    /// connection without answering. Shared by the socket server and the in-process door, so
    /// both ways in are served by the same script.
    fn scripted_respond(
        shared: &Mutex<ScriptedState>,
        method: &str,
        target: &str,
        header: &dyn Fn(&str) -> Option<String>,
        body: &[u8],
    ) -> Option<Vec<u8>> {
        let reply = |status: &str, extra: &str, body: &[u8], with_body: bool| {
            let mut out = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n",
                body.len()
            )
            .into_bytes();
            if with_body {
                out.extend_from_slice(body);
            }
            out
        };
        let mut state = shared.lock().unwrap();
        let answer: Option<Vec<u8>> = match method {
            "HEAD" if !state.existing => Some(reply("404 Not Found", "", b"", false)),
            "HEAD" => Some(reply(
                "200 OK",
                &format!("x-amz-object-size: {}\r\n", state.data.len()),
                &vec![0u8; state.data.len()],
                false,
            )),
            "POST" if target.contains("append-resolve") => {
                let attempt = target
                    .split(['?', '&'])
                    .find_map(|part| part.strip_prefix("attempt="))
                    .unwrap_or_default()
                    .to_string();
                if state.reads_fail {
                    Some(reply("503 Service Unavailable", "", b"undecided", true))
                } else if state.committed_attempts.contains(&attempt) {
                    Some(reply("200 OK", "x-append-resolution: landed\r\n", b"landed\n", true))
                } else {
                    Some(reply("200 OK", "x-append-resolution: absent\r\n", b"absent\n", true))
                }
            }
            "GET" => {
                if !state.existing {
                    Some(reply("404 Not Found", "", b"", true))
                } else if let Some(range) = header("range") {
                    let spec = range.trim_start_matches("bytes=");
                    let (first, last) = spec.split_once('-').unwrap();
                    let first: usize = first.parse().unwrap();
                    if first >= state.data.len() {
                        Some(reply(
                            "416 Range Not Satisfiable",
                            &format!("Content-Range: bytes */{}\r\n", state.data.len()),
                            b"",
                            true,
                        ))
                    } else {
                        let last = last
                            .parse::<usize>()
                            .unwrap_or(usize::MAX)
                            .min(state.data.len() - 1);
                        Some(reply("206 Partial Content", "", &state.data[first..=last], true))
                    }
                } else {
                    Some(reply("200 OK", "", &state.data.clone(), true))
                }
            }
            "PUT" => {
                state.puts += 1;
                let attempt_id = header("x-amz-attempt-id").expect("an append carries its attempt id");
                assert_eq!(attempt_id.len(), 32, "{attempt_id}");
                assert!(attempt_id.chars().all(|c| c.is_ascii_hexdigit()), "{attempt_id}");
                state.put_attempts.push(attempt_id.clone());
                let fault = if state.puts == 1 { state.fault_on_first_put } else { Fault::None };
                let offset: usize = header("x-amz-write-offset-bytes")
                    .and_then(|value| value.parse().ok())
                    .expect("an append carries its offset");
                if offset != state.data.len() {
                    Some(reply("412 Precondition Failed", "", b"offset", true))
                } else if fault == Fault::Refuse503 {
                    Some(reply("503 Service Unavailable", "", b"try later", true))
                } else {
                    state.data.extend_from_slice(body);
                    state.committed_attempts.push(attempt_id);
                    state.existing = true;
                    match fault {
                        Fault::CommitThenDrop => None,
                        Fault::CommitThen503 => Some(reply(
                            "503 Service Unavailable",
                            "",
                            b"the publication is unconfirmed",
                            true,
                        )),
                        _ => Some(reply(
                            "200 OK",
                            &format!("x-amz-object-size: {}\r\n", state.data.len()),
                            b"",
                            false,
                        )),
                    }
                }
            }
            _ => Some(reply("405 Method Not Allowed", "", b"", true)),
        };
        drop(state);
        answer
    }

    fn scripted_server(
        fault_on_first_put: Fault,
        reads_fail: bool,
    ) -> (String, Arc<Mutex<ScriptedState>>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(ScriptedState {
            data: Vec::new(),
            existing: false,
            puts: 0,
            fault_on_first_put,
            reads_fail,
            committed_attempts: Vec::new(),
            put_attempts: Vec::new(),
        }));
        let shared = state.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut raw = Vec::new();
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    let n = stream.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        break None;
                    }
                    raw.extend_from_slice(&chunk[..n]);
                    if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break Some(at + 4);
                    }
                };
                let Some(head_end) = head_end else { continue };
                let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
                let mut lines = head.lines();
                let request_line = lines.next().unwrap_or_default().to_string();
                let method = request_line.split(' ').next().unwrap_or_default().to_string();
                let target = request_line.split(' ').nth(1).unwrap_or_default().to_string();
                let header = |name: &str| -> Option<String> {
                    head.lines().find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case(name).then(|| value.trim().to_string())
                    })
                };
                let content_length: usize = header("content-length")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0);
                while raw.len() < head_end + content_length {
                    let n = stream.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&chunk[..n]);
                }
                let body = raw[head_end..].to_vec();
                let answer = scripted_respond(&shared, &method, &target, &header, &body);
                if let Some(answer) = answer {
                    let _ = stream.write_all(&answer);
                }
            }
        });
        (format!("matrixobject://{addr}"), state)
    }

    #[tokio::test]
    async fn an_append_committed_before_the_socket_dropped_is_reported_landed_not_repeated() {
        let (uri, state) = scripted_server(Fault::CommitThenDrop, false);
        let store = MatrixObjectHttpStore::new(&uri).unwrap();
        let receipt = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("the bytes are in the object, so the append succeeded");
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 10));
        assert_eq!(receipt.object_length, 10);
        let state = state.lock().unwrap();
        assert_eq!(state.data, b"record-one", "stored exactly once");
        assert_eq!(state.puts, 1, "nothing was sent a second time");
    }

    #[tokio::test]
    async fn an_append_committed_before_a_503_is_reported_landed_not_repeated() {
        let (uri, state) = scripted_server(Fault::CommitThen503, false);
        let store = MatrixObjectHttpStore::new(&uri).unwrap();
        let receipt = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("a 503 after the commit must be settled by reading");
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 10));
        let state = state.lock().unwrap();
        assert_eq!(state.data, b"record-one", "stored exactly once");
    }

    #[tokio::test]
    async fn an_append_refused_with_a_503_and_not_committed_is_retried_once_it_is_known_absent() {
        let (uri, state) = scripted_server(Fault::Refuse503, false);
        let store = MatrixObjectHttpStore::new(&uri).unwrap();
        let receipt = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("nothing was committed, so the retry is safe and lands");
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 10));
        let state = state.lock().unwrap();
        assert_eq!(state.data, b"record-one");
        assert_eq!(state.puts, 2);
    }

    #[tokio::test]
    async fn an_append_that_cannot_be_read_back_reports_an_unknown_outcome_and_is_not_repeated() {
        let (uri, state) = scripted_server(Fault::CommitThenDrop, true);
        let store = MatrixObjectHttpStore::new(&uri).unwrap();
        let error = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect_err("whether it landed cannot be established");
        assert!(
            matches!(error, ObjectStoreError::OutcomeUnknown { .. }),
            "got {error:?}"
        );
        let state = state.lock().unwrap();
        assert_eq!(state.puts, 1, "an append of unknown outcome is never sent again");
        assert_eq!(state.data, b"record-one");
    }

    // ---- A door that cannot answer ----

    /// A transport that never answers: the door fell over.
    #[derive(Debug)]
    struct DeadDoor;

    impl Transport for DeadDoor {
        fn send(
            &self,
            _method: &str,
            _target: &str,
            _headers: &[(&str, String)],
            _body: &[u8],
        ) -> Result<matrixobject_client::Reply, matrixobject_client::TransportError> {
            Err(matrixobject_client::TransportError("the door fell over".to_string()))
        }
    }

    #[tokio::test]
    async fn a_door_that_cannot_answer_is_a_transport_failure_with_its_own_words_and_not_a_503() {
        let store = NetworkedMatrixObject::direct(Arc::new(DeadDoor), "temporalstore");
        let error = store.get("wal").await.expect_err("the door did not answer");
        let text = error.to_string();
        assert!(text.contains("the door fell over"), "the door's own failure is reported: {text}");
        assert!(!text.contains("503"), "a door that fails to answer is not an answered 503: {text}");
        assert_eq!(store.requests_issued(), 1);
    }

    // ---- The networked store and its mode ----

    #[test]
    fn direct_is_the_default_and_an_explicit_mode_wins() {
        let gw = Some("matrixobject://host:9000");
        let meta = Some("m1:7000,m2:7000");
        let r = resolve_matrixobject_mode;
        // Nothing set: no networked store, which is today's behaviour byte for byte.
        assert_eq!(r(None, None, None), Ok(None));
        assert_eq!(r(Some(" "), Some(""), Some("  ")), Ok(None));
        // A URI of another scheme is not a gateway.
        assert_eq!(r(None, Some("s3://bucket"), None), Ok(None));
        // One side set picks that side.
        assert_eq!(r(None, gw, None), Ok(Some(MatrixObjectMode::Gateway)));
        assert_eq!(r(None, None, meta), Ok(Some(MatrixObjectMode::Direct)));
        // Both set and no mode: direct is the default.
        assert_eq!(r(None, gw, meta), Ok(Some(MatrixObjectMode::Direct)));
        // An explicit mode must have what it needs, and wins over the other setting.
        assert_eq!(r(Some("Gateway"), gw, None), Ok(Some(MatrixObjectMode::Gateway)));
        assert_eq!(r(Some("direct"), None, meta), Ok(Some(MatrixObjectMode::Direct)));
        assert_eq!(r(Some("gateway"), None, meta), Err(MatrixObjectModeError::GatewayNeedsUri));
        assert_eq!(r(Some("direct"), gw, None), Err(MatrixObjectModeError::DirectNeedsMeta));
        assert_eq!(r(Some("gateway"), gw, meta), Ok(Some(MatrixObjectMode::Gateway)));
        assert_eq!(r(Some("direct"), gw, meta), Ok(Some(MatrixObjectMode::Direct)));
        // A mode name's case does not matter.
        assert_eq!(r(Some("DIRECT"), gw, meta), Ok(Some(MatrixObjectMode::Direct)));
        assert!(matches!(
            r(Some("both"), gw, None),
            Err(MatrixObjectModeError::UnknownMode(_))
        ));
    }

    #[test]
    fn direct_mode_is_refused_loudly_until_the_client_is_built_in() {
        let error = NetworkedMatrixObject::connect(MatrixObjectMode::Direct, "m1:7000")
            .expect_err("no direct client yet");
        assert!(error.to_string().contains("matrixobject_direct::DirectConfig"), "{error}");
    }

    #[tokio::test]
    async fn the_networked_store_forwards_every_method_to_the_gateway_store() {
        let (uri, state) = scripted_server(Fault::None, false);
        let store = NetworkedMatrixObject::connect(MatrixObjectMode::Gateway, &uri).unwrap();
        assert_eq!(store.mode(), MatrixObjectMode::Gateway);
        // The defaulted methods: an append and a ranged tail read reach the server and answer
        // as the inner store would, rather than falling back to the trait's refusing default.
        let receipt = store.append_blob("wal", Bytes::from_static(b"abcdef")).await.unwrap();
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 6));
        assert_eq!(store.get_range_from("wal", 2).await.unwrap().unwrap().as_ref(), b"cdef");
        assert_eq!(store.get_range("wal", 1, 3).await.unwrap().as_ref(), b"bcd");
        assert_eq!(BlockingObjectRead::get_blocking(&store, "wal").unwrap().as_ref(), b"abcdef");
        assert_eq!(state.lock().unwrap().data, b"abcdef");
        // compare_and_swap is the one the trait refuses by default; a forwarding wrapper must
        // report the inner store's answer, which for the gateway store is also a refusal -- so
        // check it is the INNER message, not the trait default's.
        let inner = MatrixObjectHttpStore::new(&uri).unwrap();
        let direct = inner.compare_and_swap("k", None, Bytes::from_static(b"x")).await;
        let wrapped = store.compare_and_swap("k", None, Bytes::from_static(b"x")).await;
        assert_eq!(format!("{direct:?}"), format!("{wrapped:?}"));
    }

    /// The scripted door, reached in process: the same answers, no socket.
    #[derive(Debug)]
    struct ScriptedDoor(Arc<Mutex<ScriptedState>>);

    impl Transport for ScriptedDoor {
        fn send(
            &self,
            method: &str,
            target: &str,
            headers: &[(&str, String)],
            body: &[u8],
        ) -> Result<matrixobject_client::Reply, matrixobject_client::TransportError> {
            let header = |name: &str| -> Option<String> {
                headers
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case(name))
                    .map(|(_, value)| value.clone())
            };
            let raw = scripted_respond(&self.0, method, target, &header, body).ok_or_else(|| {
                matrixobject_client::TransportError("the door dropped the request".to_string())
            })?;
            let head_end = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("a reply head") + 4;
            let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
            let mut lines = head.lines();
            let status = lines
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|code| code.parse().ok())
                .expect("a status line");
            let headers = lines
                .filter_map(|line| line.split_once(':'))
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .collect();
            Ok(matrixobject_client::Reply { status, headers, body: raw[head_end..].to_vec() })
        }
    }

    fn scripted_direct(
        fault_on_first_put: Fault,
        reads_fail: bool,
    ) -> (NetworkedMatrixObject, Arc<Mutex<ScriptedState>>) {
        let state = Arc::new(Mutex::new(ScriptedState {
            data: Vec::new(),
            existing: false,
            puts: 0,
            fault_on_first_put,
            reads_fail,
            committed_attempts: Vec::new(),
            put_attempts: Vec::new(),
        }));
        let store = NetworkedMatrixObject::direct(Arc::new(ScriptedDoor(state.clone())), "temporalstore");
        (store, state)
    }

    fn scripted_gateway(
        fault_on_first_put: Fault,
        reads_fail: bool,
    ) -> (NetworkedMatrixObject, Arc<Mutex<ScriptedState>>) {
        let (uri, state) = scripted_server(fault_on_first_put, reads_fail);
        (NetworkedMatrixObject::connect(MatrixObjectMode::Gateway, &uri).unwrap(), state)
    }

    /// One sequence of writes, run through any store, reporting everything a caller can see.
    async fn drive<S: ObjectStore>(store: &S) -> Vec<String> {
        let mut seen = Vec::new();
        seen.push(format!("absent get: {:?}", store.get("wal").await.map_err(|e| e.to_string())));
        for record in [&b"alpha"[..], b"", b"bravo-bravo", b"c"] {
            let receipt = store.append_blob("wal", Bytes::copy_from_slice(record)).await;
            seen.push(match receipt {
                Ok(r) => format!(
                    "append {} -> start {} end {} written {} length {} slabs {} first {:?}",
                    record.len(),
                    r.start_offset,
                    r.end_offset,
                    r.bytes_written,
                    r.object_length,
                    r.physical_slab_count,
                    r.first_physical_offset
                ),
                Err(e) => format!("append {} -> error {e}", record.len()),
            });
        }
        seen.push(format!("get {:?}", store.get("wal").await.unwrap()));
        seen.push(format!("range 3..8 {:?}", store.get_range("wal", 3, 5).await.unwrap()));
        seen.push(format!("range 0..0 {:?}", store.get_range("wal", 0, 0).await.unwrap()));
        seen.push(format!("range_from 5 {:?}", store.get_range_from("wal", 5).await.unwrap()));
        seen.push(format!("range_from end {:?}", store.get_range_from("wal", 17).await.unwrap()));
        seen.push(format!("range_from past {:?}", store.get_range_from("wal", 18).await.unwrap()));
        seen
    }

    #[tokio::test]
    async fn identical_bytes_in_both_modes() {
        let (gateway, gateway_state) = scripted_gateway(Fault::None, false);
        let (direct, direct_state) = scripted_direct(Fault::None, false);
        assert_eq!(direct.mode(), MatrixObjectMode::Direct);
        let through_gateway = drive(&gateway).await;
        let through_direct = drive(&direct).await;
        // What the caller saw: receipts, reads, ranges, tails.
        assert_eq!(through_gateway, through_direct);
        // What is durably in the object: the bytes themselves.
        assert_eq!(gateway_state.lock().unwrap().data, direct_state.lock().unwrap().data);
        assert_eq!(direct_state.lock().unwrap().data, b"alphabravo-bravoc");
        // The control: the drive wrote and read something, so two empty lists would not pass.
        assert!(through_gateway.len() >= 10, "{through_gateway:?}");
        // The direct way sent nothing over a wire; the gateway way did.
        assert_eq!(direct.wire_bytes_sent(), 0);
        assert!(gateway.wire_bytes_sent() > 0);
    }

    /// The same fault in each way in leaves the same bytes and the same answer.
    #[tokio::test]
    async fn an_append_that_committed_before_a_failure_leaves_the_same_bytes_in_both_modes() {
        for fault in [Fault::CommitThen503, Fault::CommitThenDrop, Fault::Refuse503] {
            let (gateway, gateway_state) = scripted_gateway(fault, false);
            let (direct, direct_state) = scripted_direct(fault, false);
            let a = gateway.append_blob("wal", Bytes::from_static(b"record-one")).await.unwrap();
            let b = direct.append_blob("wal", Bytes::from_static(b"record-one")).await.unwrap();
            assert_eq!(
                (a.start_offset, a.end_offset, a.object_length),
                (b.start_offset, b.end_offset, b.object_length),
                "{fault:?}"
            );
            assert_eq!(gateway_state.lock().unwrap().data, direct_state.lock().unwrap().data, "{fault:?}");
            assert_eq!(direct_state.lock().unwrap().data, b"record-one", "stored exactly once ({fault:?})");
            assert_eq!(
                gateway_state.lock().unwrap().puts,
                direct_state.lock().unwrap().puts,
                "the same number of requests reached the door ({fault:?})"
            );
        }
    }

    #[tokio::test]
    async fn an_append_that_cannot_be_read_back_is_unknown_in_both_modes_and_is_not_repeated() {
        let (gateway, gateway_state) = scripted_gateway(Fault::CommitThenDrop, true);
        let (direct, direct_state) = scripted_direct(Fault::CommitThenDrop, true);
        for (store, state) in [(&gateway, &gateway_state), (&direct, &direct_state)] {
            let error = store
                .append_blob("wal", Bytes::from_static(b"record-one"))
                .await
                .expect_err("whether it landed cannot be established");
            assert!(matches!(error, ObjectStoreError::OutcomeUnknown { .. }), "got {error:?}");
            assert_eq!(state.lock().unwrap().puts, 1, "never sent again");
        }
    }

    /// A long run of sequential appends must not wedge or diverge in either mode: each lands at
    /// the end the last one left, every receipt agrees with the other mode's, and the bytes match.
    #[tokio::test]
    async fn three_thousand_sequential_appends_agree_in_both_modes() {
        const APPENDS: usize = 3000;
        const SIZE: usize = 1024;
        let (gateway, gateway_state) = scripted_gateway(Fault::None, false);
        let (direct, direct_state) = scripted_direct(Fault::None, false);
        for index in 0..APPENDS {
            let record = vec![(index % 251) as u8; SIZE];
            let a = gateway
                .append_blob("wal", Bytes::from(record.clone()))
                .await
                .unwrap_or_else(|e| panic!("gateway append {index}: {e}"));
            let b = direct
                .append_blob("wal", Bytes::from(record))
                .await
                .unwrap_or_else(|e| panic!("direct append {index}: {e}"));
            assert_eq!(
                (a.start_offset, a.end_offset, a.object_length),
                (b.start_offset, b.end_offset, b.object_length),
                "receipts diverged at append {index}"
            );
            assert_eq!(a.start_offset, (index * SIZE) as u64, "append {index} landed off the end");
        }
        assert_eq!(direct_state.lock().unwrap().data.len(), APPENDS * SIZE);
        assert!(
            gateway_state.lock().unwrap().data == direct_state.lock().unwrap().data,
            "the stored bytes differ"
        );
        // One request per append once the length is known: neither way pays a HEAD each time.
        assert_eq!(direct.requests_issued() as usize, APPENDS + 1);
        assert_eq!(gateway.requests_issued() as usize, APPENDS + 1);
    }

    /// A door that fails to answer is a transport failure, settled by reading and not repeated.
    #[tokio::test]
    async fn a_door_that_fails_to_answer_is_settled_by_reading_the_object() {
        let (direct, state) = scripted_direct(Fault::CommitThenDrop, false);
        let receipt = direct
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("the bytes are in the object, so the append succeeded");
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 10));
        assert_eq!(state.lock().unwrap().puts, 1, "nothing was sent a second time");
    }
}
