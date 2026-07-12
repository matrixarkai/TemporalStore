use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("invalid object key: {0}")]
    InvalidKey(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub key: String,
    pub uri: String,
    pub size_bytes: u64,
    pub checksum_sha256: String,
}

impl ObjectMetadata {
    pub fn from_bytes(key: &str, uri: String, bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self {
            key: key.to_string(),
            uri,
            size_bytes: bytes.len() as u64,
            checksum_sha256: hex::encode(hasher.finalize()),
        }
    }
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError>;
    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError>;
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;
    fn uri(&self, key: &str) -> String;

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectStoreError> {
        let bytes = self.get(key).await?;
        Ok(ObjectMetadata::from_bytes(key, self.uri(key), &bytes))
    }

    async fn put_atomic(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(key, bytes).await?;
        self.head(key).await
    }
}

#[derive(Debug, Clone)]
pub struct FileObjectStore {
    root: PathBuf,
    uri_scheme: String,
    created_dirs: Arc<Mutex<HashSet<PathBuf>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatrixObjectStoreBackendMode {
    LocalCompat,
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixObjectStoreConfig {
    pub root: PathBuf,
    pub uri_scheme: String,
    pub endpoint: Option<String>,
    pub backend_mode: MatrixObjectStoreBackendMode,
}

impl MatrixObjectStoreConfig {
    pub fn local_compat(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            uri_scheme: "matrixobjectstore".to_string(),
            endpoint: None,
            backend_mode: MatrixObjectStoreBackendMode::LocalCompat,
        }
    }

    pub fn external(endpoint: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            uri_scheme: "matrixobjectstore".to_string(),
            endpoint: Some(endpoint.into()),
            backend_mode: MatrixObjectStoreBackendMode::External,
        }
    }

    pub fn with_uri_scheme(mut self, uri_scheme: impl Into<String>) -> Self {
        self.uri_scheme = uri_scheme.into();
        self
    }
}

#[derive(Debug, Clone)]
pub struct MatrixObjectStore {
    config: MatrixObjectStoreConfig,
    local_compat: FileObjectStore,
}

impl MatrixObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::from_config(MatrixObjectStoreConfig::local_compat(root))
    }

    pub fn with_uri_scheme(root: impl Into<PathBuf>, uri_scheme: impl Into<String>) -> Self {
        Self::from_config(MatrixObjectStoreConfig::local_compat(root).with_uri_scheme(uri_scheme))
    }

    pub fn from_config(config: MatrixObjectStoreConfig) -> Self {
        let local_compat = FileObjectStore::with_uri_scheme(&config.root, &config.uri_scheme);
        Self {
            config,
            local_compat,
        }
    }

    pub fn config(&self) -> &MatrixObjectStoreConfig {
        &self.config
    }
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
}

#[async_trait]
impl ObjectStore for FileObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.put_atomic(key, bytes).await.map(|_| ())
    }

    async fn put_atomic(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let path = self.resolve(key)?;
        if let Some(parent) = path.parent() {
            let should_create = {
                let mut created = self
                    .created_dirs
                    .lock()
                    .expect("object-store dir cache poisoned");
                created.insert(parent.to_path_buf())
            };
            if should_create {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let tmp_path =
            path.with_extension(format!("matrixobjectstore-tmp-{}", Uuid::new_v4().simple()));
        let mut file = tokio::fs::File::create(&tmp_path).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        match tokio::fs::rename(&tmp_path, &path).await {
            Ok(()) => {}
            Err(err) => {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                return Err(ObjectStoreError::Io(err));
            }
        }
        Ok(ObjectMetadata::from_bytes(key, self.uri(key), &bytes))
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectStoreError> {
        let bytes = self.get(key).await?;
        Ok(ObjectMetadata::from_bytes(key, self.uri(key), &bytes))
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

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let mut out = Vec::new();
        let root = self.root.clone();
        if !root.exists() {
            return Ok(out);
        }
        collect_files(&root, &root, &mut out).await?;
        out.retain(|key| key.starts_with(prefix) && !key.contains(".matrixobjectstore-tmp-"));
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

#[async_trait]
impl ObjectStore for MatrixObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.local_compat.put(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        self.local_compat.get(key).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.local_compat.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.local_compat.delete(key).await
    }

    fn uri(&self, key: &str) -> String {
        self.local_compat.uri(key)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectStoreError> {
        self.local_compat.head(key).await
    }

    async fn put_atomic(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.local_compat.put_atomic(key, bytes).await
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

#[cfg(test)]
mod tests {
    use super::{
        FileObjectStore, MatrixObjectStore, MatrixObjectStoreBackendMode, MatrixObjectStoreConfig,
        ObjectStore, ObjectStoreError,
    };
    use bytes::Bytes;

    #[tokio::test]
    async fn matrix_object_store_put_atomic_returns_checksum_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = MatrixObjectStore::with_uri_scheme(dir.path(), "matrixobjectstore");

        let metadata = store
            .put_atomic(
                "tenant/a/blob-1",
                Bytes::from_static(b"hello matrix object store"),
            )
            .await
            .unwrap();

        assert_eq!(metadata.key, "tenant/a/blob-1");
        assert_eq!(metadata.uri, "matrixobjectstore://tenant/a/blob-1");
        assert_eq!(metadata.size_bytes, 25);
        assert_eq!(
            metadata.checksum_sha256,
            "32bf29e5bb7440b15303a464d7e8e0c4e2a94c026e0d9820bdba0a6a8a0dc5a9"
        );
        assert_eq!(
            store.get("tenant/a/blob-1").await.unwrap(),
            Bytes::from_static(b"hello matrix object store")
        );
        assert_eq!(store.head("tenant/a/blob-1").await.unwrap(), metadata);
    }

    #[tokio::test]
    async fn matrix_object_store_overwrite_is_atomic_and_listable() {
        let dir = tempfile::tempdir().unwrap();
        let store = MatrixObjectStore::new(dir.path());

        store
            .put("raw/event-1", Bytes::from_static(b"old"))
            .await
            .unwrap();
        let updated = store
            .put_atomic("raw/event-1", Bytes::from_static(b"new-value"))
            .await
            .unwrap();

        assert_eq!(updated.size_bytes, 9);
        assert_eq!(
            store.get("raw/event-1").await.unwrap(),
            Bytes::from_static(b"new-value")
        );
        assert_eq!(
            store.list("raw/").await.unwrap(),
            vec!["raw/event-1".to_string()]
        );
    }

    #[tokio::test]
    async fn matrix_object_store_keeps_explicit_backend_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::external("matrixobjectstore://cluster-a", dir.path())
            .with_uri_scheme("matrixobjectstore");
        let store = MatrixObjectStore::from_config(config.clone());

        assert_eq!(store.config(), &config);
        assert_eq!(
            store.config().backend_mode,
            MatrixObjectStoreBackendMode::External
        );
        assert_eq!(
            store.config().endpoint.as_deref(),
            Some("matrixobjectstore://cluster-a")
        );
        let metadata = store
            .put_atomic("snapshots/a", Bytes::from_static(b"payload"))
            .await
            .unwrap();
        assert_eq!(metadata.uri, "matrixobjectstore://snapshots/a");
    }

    #[tokio::test]
    async fn matrix_object_store_rejects_path_escape_keys() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileObjectStore::new(dir.path());

        let err = store
            .put("../escape", Bytes::from_static(b"bad"))
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::InvalidKey(_)));
    }
}
