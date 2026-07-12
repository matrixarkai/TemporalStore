use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("invalid object key: {0}")]
    InvalidKey(String),
    #[error("object-store backend {backend} is not linked for {uri}")]
    UnsupportedBackend { backend: String, uri: String },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SharedObjectStoreBackend {
    LocalFile,
    SharedFile,
    MatrixObjectStore,
    S3,
    CephS3,
    CephRados,
    Unknown,
}

impl SharedObjectStoreBackend {
    pub fn parse(value: &str) -> Self {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "local_file" | "local_fs" | "file" | "file_object_store" => Self::LocalFile,
            "shared_file" | "shared" | "shared-file" | "efs" | "nfs" => Self::SharedFile,
            "matrixobjectstore"
            | "matrix_object_store"
            | "matrixobjectstore_local_compat"
            | "blob"
            | "local" => Self::MatrixObjectStore,
            "s3" => Self::S3,
            "ceph" | "ceph_s3" | "ceph+s3" => Self::CephS3,
            "rados" | "ceph_rados" => Self::CephRados,
            _ => Self::Unknown,
        }
    }

    pub fn from_uri(uri: &str) -> Self {
        let Some((scheme, _)) = uri.split_once("://") else {
            return Self::parse(uri);
        };
        Self::parse(scheme)
    }

    pub fn canonical_name(self) -> &'static str {
        match self {
            Self::LocalFile => "local_file",
            Self::SharedFile => "shared_file",
            Self::MatrixObjectStore => "matrixobjectstore",
            Self::S3 => "s3",
            Self::CephS3 => "ceph_s3",
            Self::CephRados => "ceph_rados",
            Self::Unknown => "unknown",
        }
    }

    pub fn uri_scheme(self) -> &'static str {
        match self {
            Self::LocalFile => "file",
            Self::SharedFile => "shared-file",
            Self::MatrixObjectStore => "matrixobjectstore",
            Self::S3 => "s3",
            Self::CephS3 => "ceph+s3",
            Self::CephRados => "rados",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedObjectStoreConfig {
    pub backend: SharedObjectStoreBackend,
    pub uri: String,
    pub root: PathBuf,
    pub endpoint: Option<String>,
}

impl SharedObjectStoreConfig {
    pub fn new(
        backend: SharedObjectStoreBackend,
        uri: impl Into<String>,
        root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            backend,
            uri: uri.into(),
            root: root.into(),
            endpoint: None,
        }
    }

    pub fn from_backend_and_root(
        backend: SharedObjectStoreBackend,
        root: impl Into<PathBuf>,
    ) -> Self {
        let root = root.into();
        Self::new(backend, format!("{}://", backend.uri_scheme()), root)
    }

    pub fn from_uri(uri: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        let uri = uri.into();
        Self::new(SharedObjectStoreBackend::from_uri(&uri), uri, root)
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn canonical_backend_name(&self) -> &'static str {
        self.backend.canonical_name()
    }

    pub fn uri_scheme(&self) -> &'static str {
        self.backend.uri_scheme()
    }
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
    root_service: MatrixObjectStoreRootService,
    block_service: MatrixObjectStoreBlockService,
    chunk_service: MatrixObjectStoreChunkService,
}

#[derive(Debug, Clone)]
pub struct MatrixObjectStoreRootService {
    manifest_store: FileObjectStore,
    uri_scheme: String,
}

#[derive(Debug, Clone)]
pub struct MatrixObjectStoreBlockService {
    block_store: FileObjectStore,
}

#[derive(Debug, Clone)]
pub struct MatrixObjectStoreChunkService {
    chunk_store: FileObjectStore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixObjectManifest {
    pub key: String,
    pub uri: String,
    pub size_bytes: u64,
    pub checksum_sha256: String,
    pub created_at_ms: u64,
    pub blocks: Vec<MatrixObjectBlockRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixObjectBlockRef {
    pub block_id: String,
    pub chunk_key: String,
    pub offset: u64,
    pub length: u64,
    pub checksum_sha256: String,
}

#[derive(Debug, Clone)]
pub enum SharedObjectStore {
    LocalFile(FileObjectStore),
    SharedFile(FileObjectStore),
    MatrixObjectStore(MatrixObjectStore),
}

impl SharedObjectStore {
    pub fn from_config(config: SharedObjectStoreConfig) -> Result<Self, ObjectStoreError> {
        match config.backend {
            SharedObjectStoreBackend::LocalFile => Ok(Self::LocalFile(FileObjectStore::new(
                config.root.join("objects"),
            ))),
            SharedObjectStoreBackend::SharedFile => Ok(Self::SharedFile(
                FileObjectStore::with_uri_scheme(config.root.join("objects"), "shared-file"),
            )),
            SharedObjectStoreBackend::MatrixObjectStore => Ok(Self::MatrixObjectStore(
                MatrixObjectStore::from_config(MatrixObjectStoreConfig {
                    root: config.root.join("objects"),
                    uri_scheme: SharedObjectStoreBackend::MatrixObjectStore
                        .uri_scheme()
                        .to_string(),
                    endpoint: config.endpoint,
                    backend_mode: MatrixObjectStoreBackendMode::LocalCompat,
                }),
            )),
            SharedObjectStoreBackend::S3
            | SharedObjectStoreBackend::CephS3
            | SharedObjectStoreBackend::CephRados
            | SharedObjectStoreBackend::Unknown => Err(ObjectStoreError::UnsupportedBackend {
                backend: config.canonical_backend_name().to_string(),
                uri: config.uri,
            }),
        }
    }

    pub fn from_backend_root(
        backend: SharedObjectStoreBackend,
        root: impl Into<PathBuf>,
    ) -> Result<Self, ObjectStoreError> {
        Self::from_config(SharedObjectStoreConfig::from_backend_and_root(
            backend, root,
        ))
    }
}

impl MatrixObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::from_config(MatrixObjectStoreConfig::local_compat(root))
    }

    pub fn with_uri_scheme(root: impl Into<PathBuf>, uri_scheme: impl Into<String>) -> Self {
        Self::from_config(MatrixObjectStoreConfig::local_compat(root).with_uri_scheme(uri_scheme))
    }

    pub fn from_config(config: MatrixObjectStoreConfig) -> Self {
        let root_service = MatrixObjectStoreRootService::new(
            config.root.join("_matrixobjectstore/root"),
            &config.uri_scheme,
        );
        let block_service =
            MatrixObjectStoreBlockService::new(config.root.join("_matrixobjectstore/blocks"));
        let chunk_service =
            MatrixObjectStoreChunkService::new(config.root.join("_matrixobjectstore/chunks"));
        Self {
            config,
            root_service,
            block_service,
            chunk_service,
        }
    }

    pub fn config(&self) -> &MatrixObjectStoreConfig {
        &self.config
    }
}
impl MatrixObjectStoreRootService {
    pub fn new(root: impl Into<PathBuf>, uri_scheme: impl Into<String>) -> Self {
        Self {
            manifest_store: FileObjectStore::with_uri_scheme(root, "matrixobjectstore-root"),
            uri_scheme: uri_scheme.into(),
        }
    }

    async fn put_manifest(&self, manifest: &MatrixObjectManifest) -> Result<(), ObjectStoreError> {
        self.manifest_store
            .put(
                &manifest_key(&manifest.key),
                Bytes::from(serde_json::to_vec(manifest).map_err(std::io::Error::other)?),
            )
            .await
    }

    async fn get_manifest(&self, key: &str) -> Result<MatrixObjectManifest, ObjectStoreError> {
        let bytes = self.manifest_store.get(&manifest_key(key)).await?;
        serde_json::from_slice(&bytes)
            .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))
    }

    async fn list_manifest_keys(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let mut keys = Vec::new();
        for manifest_key in self.manifest_store.list(prefix).await? {
            if let Some(key) = manifest_key
                .strip_suffix(".manifest.json")
                .filter(|key| key.starts_with(prefix))
            {
                keys.push(key.to_string());
            }
        }
        keys.sort();
        Ok(keys)
    }

    async fn delete_manifest(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.manifest_store.delete(&manifest_key(key)).await
    }

    fn uri(&self, key: &str) -> String {
        format!("{}://{}", self.uri_scheme, key)
    }
}

impl MatrixObjectStoreBlockService {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            block_store: FileObjectStore::with_uri_scheme(root, "matrixobjectstore-block"),
        }
    }

    async fn put_block_ref(
        &self,
        block_ref: &MatrixObjectBlockRef,
    ) -> Result<(), ObjectStoreError> {
        self.block_store
            .put(
                &block_manifest_key(&block_ref.block_id),
                Bytes::from(serde_json::to_vec(block_ref).map_err(std::io::Error::other)?),
            )
            .await
    }

    async fn get_block_ref(
        &self,
        block_id: &str,
    ) -> Result<MatrixObjectBlockRef, ObjectStoreError> {
        let bytes = self.block_store.get(&block_manifest_key(block_id)).await?;
        serde_json::from_slice(&bytes)
            .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))
    }

    async fn delete_block_ref(&self, block_id: &str) -> Result<(), ObjectStoreError> {
        self.block_store.delete(&block_manifest_key(block_id)).await
    }
}

impl MatrixObjectStoreChunkService {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            chunk_store: FileObjectStore::with_uri_scheme(root, "matrixobjectstore-chunk"),
        }
    }

    async fn put_chunk(
        &self,
        chunk_key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.chunk_store.put_atomic(chunk_key, bytes).await
    }

    async fn get_chunk(&self, chunk_key: &str) -> Result<Bytes, ObjectStoreError> {
        self.chunk_store.get(chunk_key).await
    }

    async fn delete_chunk(&self, chunk_key: &str) -> Result<(), ObjectStoreError> {
        self.chunk_store.delete(chunk_key).await
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

    fn list_start_dir(&self, prefix: &str) -> Result<PathBuf, ObjectStoreError> {
        if prefix.contains("..") || prefix.starts_with('/') || prefix.starts_with('\\') {
            return Err(ObjectStoreError::InvalidKey(prefix.to_string()));
        }
        if prefix.is_empty() {
            return Ok(self.root.clone());
        }
        let normalized = prefix.trim_end_matches('/');
        let dir_prefix = if prefix.ends_with('/') {
            normalized
        } else {
            normalized.rsplit_once('/').map_or("", |(parent, _)| parent)
        };
        if dir_prefix.is_empty() {
            Ok(self.root.clone())
        } else {
            Ok(self
                .root
                .join(dir_prefix.replace('/', std::path::MAIN_SEPARATOR_STR)))
        }
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
        let start_dir = self.list_start_dir(prefix)?;
        if !start_dir.exists() {
            return Ok(out);
        }
        collect_files(&root, &start_dir, &mut out).await?;
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
        self.put_atomic(key, bytes).await.map(|_| ())
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let manifest = self.root_service.get_manifest(key).await?;
        let mut out = Vec::with_capacity(manifest.size_bytes as usize);
        for block_ref in &manifest.blocks {
            let stored_block_ref = self
                .block_service
                .get_block_ref(&block_ref.block_id)
                .await?;
            let chunk = self
                .chunk_service
                .get_chunk(&stored_block_ref.chunk_key)
                .await?;
            let checksum = sha256_hex(&chunk);
            if checksum != stored_block_ref.checksum_sha256 {
                return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                    "chunk checksum mismatch for {}",
                    stored_block_ref.chunk_key
                ))));
            }
            out.extend_from_slice(&chunk);
        }
        if out.len() as u64 != manifest.size_bytes || sha256_hex(&out) != manifest.checksum_sha256 {
            return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "object checksum mismatch for {key}"
            ))));
        }
        Ok(Bytes::from(out))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.root_service.list_manifest_keys(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let manifest = match self.root_service.get_manifest(key).await {
            Ok(manifest) => manifest,
            Err(ObjectStoreError::NotFound(_)) => return Ok(()),
            Err(err) => return Err(err),
        };
        for block_ref in &manifest.blocks {
            self.chunk_service
                .delete_chunk(&block_ref.chunk_key)
                .await?;
            self.block_service
                .delete_block_ref(&block_ref.block_id)
                .await?;
        }
        self.root_service.delete_manifest(key).await
    }

    fn uri(&self, key: &str) -> String {
        self.root_service.uri(key)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectStoreError> {
        let manifest = self.root_service.get_manifest(key).await?;
        Ok(ObjectMetadata {
            key: manifest.key,
            uri: manifest.uri,
            size_bytes: manifest.size_bytes,
            checksum_sha256: manifest.checksum_sha256,
        })
    }

    async fn put_atomic(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let previous_manifest = self.root_service.get_manifest(key).await.ok();
        let checksum_sha256 = sha256_hex(&bytes);
        let block_id = format!("block-{}", checksum_sha256);
        let chunk_key = format!("{}/{}", key.trim_matches('/'), block_id);
        let chunk_metadata = self
            .chunk_service
            .put_chunk(&chunk_key, bytes.clone())
            .await?;
        let block_ref = MatrixObjectBlockRef {
            block_id,
            chunk_key,
            offset: 0,
            length: chunk_metadata.size_bytes,
            checksum_sha256: chunk_metadata.checksum_sha256,
        };
        let new_block_id = block_ref.block_id.clone();
        let new_chunk_key = block_ref.chunk_key.clone();
        self.block_service.put_block_ref(&block_ref).await?;
        let manifest = MatrixObjectManifest {
            key: key.to_string(),
            uri: self.uri(key),
            size_bytes: bytes.len() as u64,
            checksum_sha256,
            created_at_ms: now_ms(),
            blocks: vec![block_ref],
        };
        self.root_service.put_manifest(&manifest).await?;
        if let Some(previous_manifest) = previous_manifest {
            for old_block in previous_manifest.blocks {
                if old_block.chunk_key != new_chunk_key {
                    let _ = self.chunk_service.delete_chunk(&old_block.chunk_key).await;
                }
                if old_block.block_id != new_block_id {
                    let _ = self
                        .block_service
                        .delete_block_ref(&old_block.block_id)
                        .await;
                }
            }
        }
        Ok(ObjectMetadata {
            key: manifest.key,
            uri: manifest.uri,
            size_bytes: manifest.size_bytes,
            checksum_sha256: manifest.checksum_sha256,
        })
    }
}

fn manifest_key(key: &str) -> String {
    if key.is_empty() {
        String::new()
    } else {
        format!("{key}.manifest.json")
    }
}

fn block_manifest_key(block_id: &str) -> String {
    format!("{block_id}.json")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[async_trait]
impl ObjectStore for SharedObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.put(key, bytes).await,
            Self::MatrixObjectStore(store) => store.put(key, bytes).await,
        }
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.get(key).await,
            Self::MatrixObjectStore(store) => store.get(key).await,
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.list(prefix).await,
            Self::MatrixObjectStore(store) => store.list(prefix).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.delete(key).await,
            Self::MatrixObjectStore(store) => store.delete(key).await,
        }
    }

    fn uri(&self, key: &str) -> String {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.uri(key),
            Self::MatrixObjectStore(store) => store.uri(key),
        }
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.head(key).await,
            Self::MatrixObjectStore(store) => store.head(key).await,
        }
    }

    async fn put_atomic(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.put_atomic(key, bytes).await,
            Self::MatrixObjectStore(store) => store.put_atomic(key, bytes).await,
        }
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
        ObjectStore, ObjectStoreError, SharedObjectStore, SharedObjectStoreBackend,
        SharedObjectStoreConfig,
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

    #[tokio::test]
    async fn shared_object_store_backend_contract_normalizes_aliases() {
        assert_eq!(
            SharedObjectStoreBackend::from_uri("matrixobjectstore://bucket/a"),
            SharedObjectStoreBackend::MatrixObjectStore
        );
        assert_eq!(
            SharedObjectStoreBackend::from_uri("s3://bucket/a"),
            SharedObjectStoreBackend::S3
        );
        assert_eq!(
            SharedObjectStoreBackend::parse("shared-file"),
            SharedObjectStoreBackend::SharedFile
        );
        assert_eq!(
            SharedObjectStoreBackend::parse("file_object_store").canonical_name(),
            "local_file"
        );
        assert_eq!(SharedObjectStoreBackend::CephS3.uri_scheme(), "ceph+s3");
    }

    #[tokio::test]
    async fn shared_object_store_factory_uses_one_public_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = SharedObjectStoreConfig::from_backend_and_root(
            SharedObjectStoreBackend::MatrixObjectStore,
            dir.path(),
        );
        assert_eq!(config.canonical_backend_name(), "matrixobjectstore");
        assert_eq!(config.uri_scheme(), "matrixobjectstore");
        let store = SharedObjectStore::from_config(config).unwrap();

        let metadata = store
            .put_atomic("shared/key", Bytes::from_static(b"shared payload"))
            .await
            .unwrap();

        assert_eq!(metadata.uri, "matrixobjectstore://shared/key");
        assert_eq!(
            store.get("shared/key").await.unwrap(),
            Bytes::from_static(b"shared payload")
        );
    }

    #[tokio::test]
    async fn shared_object_store_remote_backends_fail_closed_until_linked() {
        let dir = tempfile::tempdir().unwrap();
        let err = SharedObjectStore::from_config(SharedObjectStoreConfig::from_uri(
            "s3://bucket/prefix",
            dir.path(),
        ))
        .unwrap_err();

        assert!(matches!(
            err,
            ObjectStoreError::UnsupportedBackend { backend, .. } if backend == "s3"
        ));
    }
}
