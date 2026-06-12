use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError>;
    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError>;
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;
    fn uri(&self, key: &str) -> String;
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
}

#[async_trait]
impl ObjectStore for FileObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
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
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(&bytes).await?;
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
    use super::*;

    #[tokio::test]
    async fn file_object_store_put_is_reopen_readable_after_durable_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileObjectStore::new(dir.path().join("objects"));

        store
            .put("shards/1/oplog/0001.json", Bytes::from_static(b"durable"))
            .await
            .unwrap();

        let reopened = tokio::fs::read(dir.path().join("objects/shards/1/oplog/0001.json"))
            .await
            .unwrap();
        assert_eq!(reopened, b"durable");
        assert_eq!(
            store.get("shards/1/oplog/0001.json").await.unwrap(),
            Bytes::from_static(b"durable")
        );
    }
}
