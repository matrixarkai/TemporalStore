// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use matrixobjectstore_rs::{MatrixObjectStore, ObjectError as MatrixObjectError, StoreOptions};
use temporalstore_snapshot::object_store::{AppendBlobReceipt, ObjectStore, ObjectStoreError};

#[derive(Debug, Clone)]
pub struct MatrixObjectObjectStore {
    bucket: String,
    content_type: String,
    inner: Arc<Mutex<MatrixObjectStore>>,
}

impl MatrixObjectObjectStore {
    pub fn new(bucket: impl Into<String>, options: StoreOptions) -> Result<Self, ObjectStoreError> {
        let bucket = bucket.into();
        let mut inner = MatrixObjectStore::new(options).map_err(map_matrixobject_error)?;
        inner
            .create_bucket(&bucket)
            .map_err(map_matrixobject_error)?;
        Ok(Self {
            bucket,
            content_type: "application/octet-stream".to_string(),
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn with_default_options(bucket: impl Into<String>) -> Result<Self, ObjectStoreError> {
        Self::new(bucket, StoreOptions::default())
    }

    /// Construct a **durable** store rooted at `dir`.
    ///
    /// `StoreOptions::default()` keeps the entire object store in memory, so the
    /// bytes vanish when the process exits. This constructor points MatrixObject
    /// at an on-disk snapshot (`<dir>/matrixobject.snapshot`) with
    /// flush-on-commit enabled: every mutating operation atomically rewrites the
    /// snapshot, and `MatrixObjectStore::new` reads it back at startup. That is
    /// what lets a datanode recover shard data from shared storage after its
    /// local dirs are wiped and it restarts.
    ///
    /// The `create_bucket` call in [`Self::new`] is idempotent, so restoring a
    /// snapshot that already contains `bucket` is safe.
    pub fn with_persistent_dir(
        bucket: impl Into<String>,
        dir: impl AsRef<Path>,
    ) -> Result<Self, ObjectStoreError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).map_err(|err| {
            ObjectStoreError::InvalidKey(format!(
                "failed to create matrixobject store dir {}: {err}",
                dir.display()
            ))
        })?;
        let snapshot_path = dir.join("matrixobject.snapshot");
        let options = StoreOptions {
            persistent_snapshot_path: snapshot_path.to_string_lossy().into_owned(),
            persistent_snapshot_flush_on_commit: true,
            ..StoreOptions::default()
        };
        Self::new(bucket, options)
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn from_snapshot_bytes(
        bucket: impl Into<String>,
        options: StoreOptions,
        snapshot_bytes: &[u8],
    ) -> Result<Self, ObjectStoreError> {
        let bucket = bucket.into();
        let mut inner = MatrixObjectStore::new(options).map_err(map_matrixobject_error)?;
        inner
            .install_snapshot_bytes(snapshot_bytes)
            .map_err(map_matrixobject_error)?;
        Ok(Self {
            bucket,
            content_type: "application/octet-stream".to_string(),
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn snapshot_bytes(&self) -> Result<Vec<u8>, ObjectStoreError> {
        let inner = self.inner.lock().expect("matrixobject lock poisoned");
        inner
            .export_snapshot()
            .to_bytes()
            .map_err(map_matrixobject_error)
    }

    pub fn inner(&self) -> Arc<Mutex<MatrixObjectStore>> {
        Arc::clone(&self.inner)
    }

    /// Synchronous read of one object, for the lazy slab read-through: the store is a mutex
    /// over in-process state, so nothing here would actually await.
    pub fn get_blocking(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let inner = self.inner.lock().expect("matrixobject lock poisoned");
        let object = inner
            .get_object(&self.bucket, key)
            .map_err(map_matrixobject_error)?;
        Ok(Bytes::from(object.data))
    }

    /// Force the durable snapshot to disk after a mutation.
    ///
    /// This is a no-op when the store has no `persistent_snapshot_path` (the
    /// in-memory default), so it is safe to call unconditionally. It exists
    /// because matrixobjectstore-rs only auto-flushes `flush_on_commit` for
    /// `append_object`/`create_bucket` — a plain `put_object` (which the
    /// shared-store WAL, index and page-slab writes all use) mutates in memory
    /// but never rewrites the snapshot. Without this explicit flush a datanode
    /// would appear durable yet lose every put on restart.
    fn flush_snapshot(inner: &MatrixObjectStore) -> Result<(), ObjectStoreError> {
        inner
            .flush_persistent_snapshot()
            .map_err(map_matrixobject_error)
    }
}

#[async_trait]
impl ObjectStore for MatrixObjectObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        let mut inner = self.inner.lock().expect("matrixobject lock poisoned");
        inner
            .put_object(&self.bucket, key, bytes.to_vec(), self.content_type.clone())
            .map_err(map_matrixobject_error)?;
        Self::flush_snapshot(&inner)?;
        Ok(())
    }

    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        let bytes_written = bytes.len() as u64;
        let mut inner = self.inner.lock().expect("matrixobject lock poisoned");
        let metadata = inner
            .append_object(&self.bucket, key, bytes.to_vec(), self.content_type.clone())
            .map_err(map_matrixobject_error)?;
        let end_offset = metadata.length;
        let start_offset = end_offset.saturating_sub(bytes_written);
        // append_object already honors flush_on_commit, but flush again so
        // durability does not depend on how the store was configured.
        Self::flush_snapshot(&inner)?;
        Ok(AppendBlobReceipt {
            key: key.to_string(),
            start_offset,
            end_offset,
            bytes_written,
            object_length: metadata.length,
            physical_band_count: metadata.extents.len(),
            first_physical_offset: metadata.extents.first().map(|extent| extent.offset),
        })
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let inner = self.inner.lock().expect("matrixobject lock poisoned");
        let object = inner
            .get_object(&self.bucket, key)
            .map_err(map_matrixobject_error)?;
        Ok(Bytes::from(object.data))
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, ObjectStoreError> {
        let inner = self.inner.lock().expect("matrixobject lock poisoned");
        let object = inner
            .get_object_range(&self.bucket, key, offset, length)
            .map_err(map_matrixobject_error)?;
        Ok(Bytes::from(object.data))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let inner = self.inner.lock().expect("matrixobject lock poisoned");
        let mut marker = None;
        let mut out = Vec::new();
        let limit = 1024;
        loop {
            let objects = inner
                .list_objects(&self.bucket, prefix, marker.as_deref(), limit)
                .map_err(map_matrixobject_error)?;
            if objects.is_empty() {
                break;
            }
            let page_len = objects.len();
            marker = objects
                .last()
                .map(|metadata| metadata.object_id.key.clone());
            out.extend(objects.into_iter().map(|metadata| metadata.object_id.key));
            if page_len < limit {
                break;
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let mut inner = self.inner.lock().expect("matrixobject lock poisoned");
        match inner.delete_object(&self.bucket, key) {
            Ok(_) | Err(MatrixObjectError::NotFound(_)) => {}
            Err(err) => return Err(map_matrixobject_error(err)),
        }
        Self::flush_snapshot(&inner)?;
        Ok(())
    }

    fn uri(&self, key: &str) -> String {
        format!("matrixobject://{}/{}", self.bucket, key)
    }
}

fn map_matrixobject_error(err: MatrixObjectError) -> ObjectStoreError {
    match err {
        MatrixObjectError::NotFound(message) => ObjectStoreError::NotFound(message),
        MatrixObjectError::InvalidArgument(message) => ObjectStoreError::InvalidKey(message),
        MatrixObjectError::PreconditionFailed(message) | MatrixObjectError::Corruption(message) => {
            ObjectStoreError::InvalidKey(message)
        }
    }
}
