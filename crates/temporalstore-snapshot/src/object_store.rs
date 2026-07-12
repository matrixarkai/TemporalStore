use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::task::JoinSet;
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
    async fn put_unique(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.put(key, bytes).await
    }
    async fn put_path_unique(
        &self,
        key: &str,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let bytes = Bytes::from(tokio::fs::read(path).await?);
        self.put_unique(key, bytes).await?;
        self.head(key).await
    }
    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError>;
    async fn get_to_path(
        &self,
        key: &str,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let bytes = self.get(key).await?;
        write_object_file(path, &bytes).await?;
        Ok(ObjectMetadata::from_bytes(key, self.uri(key), &bytes))
    }
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
    sync_writes: bool,
    sync_parent_dirs: bool,
    created_dirs: Arc<Mutex<HashSet<PathBuf>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatrixObjectStoreBackendMode {
    LocalCompat,
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MatrixObjectStoreServiceEndpoints {
    pub root_endpoint: Option<String>,
    pub block_endpoint: Option<String>,
    pub chunk_endpoint: Option<String>,
}

impl MatrixObjectStoreServiceEndpoints {
    pub fn unified(endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        Self {
            root_endpoint: Some(endpoint.clone()),
            block_endpoint: Some(endpoint.clone()),
            chunk_endpoint: Some(endpoint),
        }
    }

    pub fn split(
        root_endpoint: impl Into<String>,
        block_endpoint: impl Into<String>,
        chunk_endpoint: impl Into<String>,
    ) -> Self {
        Self {
            root_endpoint: Some(root_endpoint.into()),
            block_endpoint: Some(block_endpoint.into()),
            chunk_endpoint: Some(chunk_endpoint.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixObjectStoreConfig {
    pub root: PathBuf,
    pub uri_scheme: String,
    pub endpoint: Option<String>,
    pub backend_mode: MatrixObjectStoreBackendMode,
    #[serde(default)]
    pub service_endpoints: MatrixObjectStoreServiceEndpoints,
    #[serde(default = "default_matrixobjectstore_chunk_target_bytes")]
    pub chunk_target_bytes: usize,
    #[serde(default = "default_matrixobjectstore_transfer_concurrency")]
    pub transfer_concurrency: usize,
    #[serde(default = "default_strict_block_metadata")]
    pub verify_block_metadata_on_read: bool,
    #[serde(default = "default_publish_block_metadata")]
    pub publish_block_metadata_on_write: bool,
    #[serde(default = "default_matrixobjectstore_sync_writes")]
    pub sync_writes: bool,
    #[serde(default = "default_matrixobjectstore_sync_parent_dirs")]
    pub sync_parent_dirs: bool,
}

impl MatrixObjectStoreConfig {
    pub fn local_compat(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            uri_scheme: "matrixobjectstore".to_string(),
            endpoint: None,
            backend_mode: MatrixObjectStoreBackendMode::LocalCompat,
            service_endpoints: MatrixObjectStoreServiceEndpoints::default(),
            chunk_target_bytes: default_matrixobjectstore_chunk_target_bytes(),
            transfer_concurrency: default_matrixobjectstore_transfer_concurrency(),
            verify_block_metadata_on_read: default_strict_block_metadata(),
            publish_block_metadata_on_write: default_publish_block_metadata(),
            sync_writes: default_matrixobjectstore_sync_writes(),
            sync_parent_dirs: default_matrixobjectstore_sync_parent_dirs(),
        }
    }

    pub fn external(endpoint: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        let endpoint = endpoint.into();
        Self {
            root: root.into(),
            uri_scheme: "matrixobjectstore".to_string(),
            endpoint: Some(endpoint.clone()),
            backend_mode: MatrixObjectStoreBackendMode::External,
            service_endpoints: MatrixObjectStoreServiceEndpoints::unified(endpoint),
            chunk_target_bytes: default_matrixobjectstore_chunk_target_bytes(),
            transfer_concurrency: default_matrixobjectstore_transfer_concurrency(),
            verify_block_metadata_on_read: default_strict_block_metadata(),
            publish_block_metadata_on_write: default_publish_block_metadata(),
            sync_writes: default_matrixobjectstore_sync_writes(),
            sync_parent_dirs: default_matrixobjectstore_sync_parent_dirs(),
        }
    }

    pub fn with_uri_scheme(mut self, uri_scheme: impl Into<String>) -> Self {
        self.uri_scheme = uri_scheme.into();
        self
    }

    pub fn with_chunk_target_bytes(mut self, chunk_target_bytes: usize) -> Self {
        self.chunk_target_bytes = chunk_target_bytes.max(1);
        self
    }

    pub fn with_transfer_concurrency(mut self, transfer_concurrency: usize) -> Self {
        self.transfer_concurrency = transfer_concurrency.max(1);
        self
    }

    pub fn with_verify_block_metadata_on_read(mut self, verify: bool) -> Self {
        self.verify_block_metadata_on_read = verify;
        self
    }

    pub fn with_publish_block_metadata_on_write(mut self, publish: bool) -> Self {
        self.publish_block_metadata_on_write = publish;
        self
    }

    pub fn with_sync_writes(mut self, sync_writes: bool) -> Self {
        self.sync_writes = sync_writes;
        self
    }

    pub fn with_sync_parent_dirs(mut self, sync_parent_dirs: bool) -> Self {
        self.sync_parent_dirs = sync_parent_dirs;
        self
    }

    pub fn with_service_endpoints(
        mut self,
        service_endpoints: MatrixObjectStoreServiceEndpoints,
    ) -> Self {
        self.service_endpoints = service_endpoints;
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
    endpoint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MatrixObjectStoreBlockService {
    block_store: FileObjectStore,
    endpoint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MatrixObjectStoreChunkService {
    chunk_store: FileObjectStore,
    endpoint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixObjectStoreServiceDescriptor {
    pub service_role: String,
    pub endpoint: Option<String>,
    pub local_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixObjectStoreServiceTopology {
    pub backend_mode: MatrixObjectStoreBackendMode,
    pub root_service: MatrixObjectStoreServiceDescriptor,
    pub block_service: MatrixObjectStoreServiceDescriptor,
    pub chunk_service: MatrixObjectStoreServiceDescriptor,
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
    #[serde(default = "default_block_metadata_published")]
    pub block_metadata_published: bool,
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
                    endpoint: config.endpoint.clone(),
                    backend_mode: MatrixObjectStoreBackendMode::LocalCompat,
                    service_endpoints: config
                        .endpoint
                        .map(MatrixObjectStoreServiceEndpoints::unified)
                        .unwrap_or_default(),
                    chunk_target_bytes: default_matrixobjectstore_chunk_target_bytes(),
                    transfer_concurrency: default_matrixobjectstore_transfer_concurrency(),
                    verify_block_metadata_on_read: default_strict_block_metadata(),
                    publish_block_metadata_on_write: default_publish_block_metadata(),
                    sync_writes: default_matrixobjectstore_sync_writes(),
                    sync_parent_dirs: default_matrixobjectstore_sync_parent_dirs(),
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
            config.service_endpoints.root_endpoint.clone(),
            config.sync_writes,
            config.sync_parent_dirs,
        );
        let block_service = MatrixObjectStoreBlockService::new(
            config.root.join("_matrixobjectstore/blocks"),
            config.service_endpoints.block_endpoint.clone(),
            config.sync_writes,
            config.sync_parent_dirs,
        );
        let chunk_service = MatrixObjectStoreChunkService::new(
            config.root.join("_matrixobjectstore/chunks"),
            config.service_endpoints.chunk_endpoint.clone(),
            config.sync_writes,
            config.sync_parent_dirs,
        );
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

    pub fn service_topology(&self) -> MatrixObjectStoreServiceTopology {
        MatrixObjectStoreServiceTopology {
            backend_mode: self.config.backend_mode.clone(),
            root_service: self.root_service.descriptor(),
            block_service: self.block_service.descriptor(),
            chunk_service: self.chunk_service.descriptor(),
        }
    }
}
impl MatrixObjectStoreRootService {
    pub fn new(
        root: impl Into<PathBuf>,
        uri_scheme: impl Into<String>,
        endpoint: Option<String>,
        sync_writes: bool,
        sync_parent_dirs: bool,
    ) -> Self {
        Self {
            manifest_store: FileObjectStore::with_uri_scheme_and_sync_policy(
                root,
                "matrixobjectstore-root",
                sync_writes,
                sync_parent_dirs,
            ),
            uri_scheme: uri_scheme.into(),
            endpoint,
        }
    }

    pub fn descriptor(&self) -> MatrixObjectStoreServiceDescriptor {
        MatrixObjectStoreServiceDescriptor {
            service_role: "root".to_string(),
            endpoint: self.endpoint.clone(),
            local_root: self.manifest_store.root.clone(),
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
        for manifest_key in self
            .manifest_store
            .list_with_suffix(prefix, ".manifest.json")
            .await?
        {
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

#[derive(Debug, Clone)]
struct MatrixObjectChunkWrite {
    index: usize,
    offset: u64,
    bytes: Bytes,
}

#[derive(Debug, Clone)]
struct MatrixObjectChunkRead {
    index: usize,
    block_ref: MatrixObjectBlockRef,
}

impl MatrixObjectStoreBlockService {
    pub fn new(
        root: impl Into<PathBuf>,
        endpoint: Option<String>,
        sync_writes: bool,
        sync_parent_dirs: bool,
    ) -> Self {
        Self {
            block_store: FileObjectStore::with_uri_scheme_and_sync_policy(
                root,
                "matrixobjectstore-block",
                sync_writes,
                sync_parent_dirs,
            ),
            endpoint,
        }
    }

    pub fn descriptor(&self) -> MatrixObjectStoreServiceDescriptor {
        MatrixObjectStoreServiceDescriptor {
            service_role: "block".to_string(),
            endpoint: self.endpoint.clone(),
            local_root: self.block_store.root.clone(),
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
    pub fn new(
        root: impl Into<PathBuf>,
        endpoint: Option<String>,
        sync_writes: bool,
        sync_parent_dirs: bool,
    ) -> Self {
        Self {
            chunk_store: FileObjectStore::with_uri_scheme_and_sync_policy(
                root,
                "matrixobjectstore-chunk",
                sync_writes,
                sync_parent_dirs,
            ),
            endpoint,
        }
    }

    pub fn descriptor(&self) -> MatrixObjectStoreServiceDescriptor {
        MatrixObjectStoreServiceDescriptor {
            service_role: "chunk".to_string(),
            endpoint: self.endpoint.clone(),
            local_root: self.chunk_store.root.clone(),
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
            sync_writes: true,
            sync_parent_dirs: true,
            created_dirs: Arc::default(),
        }
    }

    pub fn with_uri_scheme(root: impl Into<PathBuf>, uri_scheme: impl Into<String>) -> Self {
        Self::with_uri_scheme_and_sync_policy(root, uri_scheme, true, true)
    }

    pub fn with_uri_scheme_and_sync_policy(
        root: impl Into<PathBuf>,
        uri_scheme: impl Into<String>,
        sync_writes: bool,
        sync_parent_dirs: bool,
    ) -> Self {
        Self {
            root: root.into(),
            uri_scheme: uri_scheme.into(),
            sync_writes,
            sync_parent_dirs,
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

    async fn list_with_suffix(
        &self,
        prefix: &str,
        suffix: &str,
    ) -> Result<Vec<String>, ObjectStoreError> {
        let mut out = Vec::new();
        let root = self.root.clone();
        if !root.exists() {
            return Ok(out);
        }
        let start_dir = self.list_start_dir(prefix)?;
        if !start_dir.exists() {
            return Ok(out);
        }
        collect_files_with_suffix(&root, &start_dir, suffix, &mut out).await?;
        out.retain(|key| key.starts_with(prefix) && !key.contains(".matrixobjectstore-tmp-"));
        out.sort();
        Ok(out)
    }

    async fn put_path_atomic(
        &self,
        key: &str,
        source: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let path = self.resolve(key)?;
        self.ensure_parent_dir(&path).await?;
        let tmp_path =
            path.with_extension(format!("matrixobjectstore-tmp-{}", Uuid::new_v4().simple()));
        let size_bytes = match tokio::fs::copy(source, &tmp_path).await {
            Ok(size_bytes) => size_bytes,
            Err(err) => return Err(ObjectStoreError::Io(err)),
        };
        let checksum_sha256 = sha256_file_hex(&tmp_path).await?;
        if self.sync_writes {
            let file = tokio::fs::File::open(&tmp_path).await?;
            file.sync_all().await?;
        }
        match tokio::fs::rename(&tmp_path, &path).await {
            Ok(()) => {
                if self.sync_parent_dirs {
                    sync_parent_dir(&path).await?;
                }
            }
            Err(err) => {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                return Err(ObjectStoreError::Io(err));
            }
        }
        Ok(ObjectMetadata {
            key: key.to_string(),
            uri: self.uri(key),
            size_bytes,
            checksum_sha256,
        })
    }

    async fn ensure_parent_dir(&self, path: &Path) -> Result<(), ObjectStoreError> {
        if let Some(parent) = path.parent() {
            let already_created = {
                let created = self
                    .created_dirs
                    .lock()
                    .expect("object-store dir cache poisoned");
                created.contains(parent)
            };
            if !already_created {
                tokio::fs::create_dir_all(parent).await?;
                if self.sync_parent_dirs {
                    sync_directory_chain(&self.root, parent).await?;
                }
                let mut created = self
                    .created_dirs
                    .lock()
                    .expect("object-store dir cache poisoned");
                created.insert(parent.to_path_buf());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for FileObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.put_atomic(key, bytes).await.map(|_| ())
    }

    async fn put_path_unique(
        &self,
        key: &str,
        source: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put_path_atomic(key, source).await
    }

    async fn put_atomic(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let path = self.resolve(key)?;
        self.ensure_parent_dir(&path).await?;
        let tmp_path =
            path.with_extension(format!("matrixobjectstore-tmp-{}", Uuid::new_v4().simple()));
        let mut file = tokio::fs::File::create(&tmp_path).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        if self.sync_writes {
            file.sync_all().await?;
        }
        drop(file);
        match tokio::fs::rename(&tmp_path, &path).await {
            Ok(()) => {
                if self.sync_parent_dirs {
                    sync_parent_dir(&path).await?;
                }
            }
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

    async fn get_to_path(
        &self,
        key: &str,
        destination: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let source = self.resolve(key)?;
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::copy(&source, destination).await {
            Ok(size_bytes) => Ok(ObjectMetadata {
                key: key.to_string(),
                uri: self.uri(key),
                size_bytes,
                checksum_sha256: sha256_file_hex(destination).await?,
            }),
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
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                if self.sync_parent_dirs {
                    sync_parent_dir(&path).await?;
                }
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(ObjectStoreError::Io(err)),
        }
    }

    fn uri(&self, key: &str) -> String {
        format!("{}://{}", self.uri_scheme, key)
    }
}

impl MatrixObjectStore {
    fn chunk_target_bytes(&self) -> usize {
        self.config.chunk_target_bytes.max(1)
    }

    fn transfer_concurrency(&self) -> usize {
        self.config.transfer_concurrency.max(1)
    }

    fn verify_block_metadata_on_read(&self) -> bool {
        self.config.verify_block_metadata_on_read
    }

    fn publish_block_metadata_on_write(&self) -> bool {
        self.config.publish_block_metadata_on_write || self.verify_block_metadata_on_read()
    }

    fn chunk_writes(&self, bytes: &Bytes) -> Vec<MatrixObjectChunkWrite> {
        let chunk_target_bytes = self.chunk_target_bytes();
        if bytes.is_empty() {
            return vec![MatrixObjectChunkWrite {
                index: 0,
                offset: 0,
                bytes: Bytes::new(),
            }];
        }
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < bytes.len() {
            let end = (offset + chunk_target_bytes).min(bytes.len());
            chunks.push(MatrixObjectChunkWrite {
                index: chunks.len(),
                offset: offset as u64,
                bytes: bytes.slice(offset..end),
            });
            offset = end;
        }
        chunks
    }

    async fn write_chunks(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<Vec<MatrixObjectBlockRef>, ObjectStoreError> {
        let chunks = self.chunk_writes(&bytes);
        let mut join_set = JoinSet::new();
        let mut next_to_submit = 0;
        let mut blocks = Vec::with_capacity(chunks.len());
        while next_to_submit < chunks.len() || !join_set.is_empty() {
            while next_to_submit < chunks.len() && join_set.len() < self.transfer_concurrency() {
                let chunk = chunks[next_to_submit].clone();
                let chunk_service = self.chunk_service.clone();
                let block_service = self.block_service.clone();
                let publish_block_metadata = self.publish_block_metadata_on_write();
                let object_key = key.trim_matches('/').to_string();
                let object_key_fingerprint = object_key_fingerprint(&object_key);
                join_set.spawn(async move {
                    let checksum_sha256 = sha256_hex(&chunk.bytes);
                    let block_id = format!(
                        "block-{}-{:020}-{}",
                        object_key_fingerprint, chunk.offset, checksum_sha256
                    );
                    let chunk_key = format!(
                        "{}/chunks/{:020}-{}",
                        object_key, chunk.offset, checksum_sha256
                    );
                    let chunk_metadata = chunk_service
                        .put_chunk(&chunk_key, chunk.bytes.clone())
                        .await?;
                    let block_ref = MatrixObjectBlockRef {
                        block_id,
                        chunk_key,
                        offset: chunk.offset,
                        length: chunk_metadata.size_bytes,
                        checksum_sha256: chunk_metadata.checksum_sha256,
                        block_metadata_published: publish_block_metadata,
                    };
                    if publish_block_metadata {
                        if let Err(err) = block_service.put_block_ref(&block_ref).await {
                            let _ = chunk_service.delete_chunk(&block_ref.chunk_key).await;
                            return Err(err);
                        }
                    }
                    Ok::<_, ObjectStoreError>((chunk.index, block_ref))
                });
                next_to_submit += 1;
            }
            let result = match join_set
                .join_next()
                .await
                .expect("matrixobjectstore chunk write task missing")
                .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))
                .and_then(|result| result)
            {
                Ok(result) => result,
                Err(err) => {
                    join_set.abort_all();
                    let partial_blocks: Vec<_> =
                        blocks.into_iter().map(|(_, block)| block).collect();
                    let _ = self.delete_block_refs(partial_blocks).await;
                    return Err(err);
                }
            };
            blocks.push(result);
        }
        blocks.sort_by_key(|(index, _)| *index);
        Ok(blocks.into_iter().map(|(_, block)| block).collect())
    }

    async fn write_chunks_from_path(
        &self,
        key: &str,
        path: &Path,
    ) -> Result<(Vec<MatrixObjectBlockRef>, String, u64), ObjectStoreError> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut buffer = vec![0u8; self.chunk_target_bytes()];
        let mut hasher = Sha256::new();
        let mut blocks = Vec::new();
        let mut join_set = JoinSet::new();
        let mut offset = 0u64;
        let mut index = 0usize;
        loop {
            let bytes_read = file.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
            let chunk = MatrixObjectChunkWrite {
                index,
                offset,
                bytes: Bytes::copy_from_slice(&buffer[..bytes_read]),
            };
            self.spawn_chunk_write(key, chunk, &mut join_set);
            index += 1;
            offset += bytes_read as u64;
            if join_set.len() >= self.transfer_concurrency() {
                let result = Self::finish_chunk_write(&mut join_set).await;
                match result {
                    Ok(result) => blocks.push(result),
                    Err(err) => {
                        join_set.abort_all();
                        let partial_blocks: Vec<_> =
                            blocks.into_iter().map(|(_, block)| block).collect();
                        let _ = self.delete_block_refs(partial_blocks).await;
                        return Err(err);
                    }
                }
            }
        }
        if index == 0 {
            self.spawn_chunk_write(
                key,
                MatrixObjectChunkWrite {
                    index: 0,
                    offset: 0,
                    bytes: Bytes::new(),
                },
                &mut join_set,
            );
        }
        while !join_set.is_empty() {
            let result = Self::finish_chunk_write(&mut join_set).await;
            match result {
                Ok(result) => blocks.push(result),
                Err(err) => {
                    join_set.abort_all();
                    let partial_blocks: Vec<_> =
                        blocks.into_iter().map(|(_, block)| block).collect();
                    let _ = self.delete_block_refs(partial_blocks).await;
                    return Err(err);
                }
            }
        }
        blocks.sort_by_key(|(index, _)| *index);
        Ok((
            blocks.into_iter().map(|(_, block)| block).collect(),
            hex::encode(hasher.finalize()),
            offset,
        ))
    }

    fn spawn_chunk_write(
        &self,
        key: &str,
        chunk: MatrixObjectChunkWrite,
        join_set: &mut JoinSet<Result<(usize, MatrixObjectBlockRef), ObjectStoreError>>,
    ) {
        let chunk_service = self.chunk_service.clone();
        let block_service = self.block_service.clone();
        let publish_block_metadata = self.publish_block_metadata_on_write();
        let object_key = key.trim_matches('/').to_string();
        let object_key_fingerprint = object_key_fingerprint(&object_key);
        join_set.spawn(async move {
            let checksum_sha256 = sha256_hex(&chunk.bytes);
            let block_id = format!(
                "block-{}-{:020}-{}",
                object_key_fingerprint, chunk.offset, checksum_sha256
            );
            let chunk_key = format!(
                "{}/chunks/{:020}-{}",
                object_key, chunk.offset, checksum_sha256
            );
            let chunk_metadata = chunk_service
                .put_chunk(&chunk_key, chunk.bytes.clone())
                .await?;
            let block_ref = MatrixObjectBlockRef {
                block_id,
                chunk_key,
                offset: chunk.offset,
                length: chunk_metadata.size_bytes,
                checksum_sha256: chunk_metadata.checksum_sha256,
                block_metadata_published: publish_block_metadata,
            };
            if publish_block_metadata {
                if let Err(err) = block_service.put_block_ref(&block_ref).await {
                    let _ = chunk_service.delete_chunk(&block_ref.chunk_key).await;
                    return Err(err);
                }
            }
            Ok::<_, ObjectStoreError>((chunk.index, block_ref))
        });
    }

    async fn finish_chunk_write(
        join_set: &mut JoinSet<Result<(usize, MatrixObjectBlockRef), ObjectStoreError>>,
    ) -> Result<(usize, MatrixObjectBlockRef), ObjectStoreError> {
        join_set
            .join_next()
            .await
            .expect("matrixobjectstore chunk write task missing")
            .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))
            .and_then(|result| result)
    }

    async fn read_chunks(
        &self,
        reads: Vec<MatrixObjectChunkRead>,
    ) -> Result<Vec<(usize, Bytes)>, ObjectStoreError> {
        let mut join_set = JoinSet::new();
        let mut next_to_submit = 0;
        let mut chunks = Vec::with_capacity(reads.len());
        while next_to_submit < reads.len() || !join_set.is_empty() {
            while next_to_submit < reads.len() && join_set.len() < self.transfer_concurrency() {
                let read = reads[next_to_submit].clone();
                let chunk_service = self.chunk_service.clone();
                let block_service = self.block_service.clone();
                let verify_block_metadata = self.verify_block_metadata_on_read();
                join_set.spawn(async move {
                    let block_ref = if verify_block_metadata {
                        block_service
                            .get_block_ref(&read.block_ref.block_id)
                            .await?
                    } else {
                        read.block_ref
                    };
                    let chunk = chunk_service.get_chunk(&block_ref.chunk_key).await?;
                    let checksum = sha256_hex(&chunk);
                    if checksum != block_ref.checksum_sha256 {
                        return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                            "chunk checksum mismatch for {}",
                            block_ref.chunk_key
                        ))));
                    }
                    Ok::<_, ObjectStoreError>((read.index, chunk))
                });
                next_to_submit += 1;
            }
            let result = join_set
                .join_next()
                .await
                .expect("matrixobjectstore chunk read task missing")
                .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))??;
            chunks.push(result);
        }
        Ok(chunks)
    }

    async fn delete_block_refs(
        &self,
        blocks: Vec<MatrixObjectBlockRef>,
    ) -> Result<(), ObjectStoreError> {
        let mut join_set = JoinSet::new();
        let mut next_to_submit = 0;
        while next_to_submit < blocks.len() || !join_set.is_empty() {
            while next_to_submit < blocks.len() && join_set.len() < self.transfer_concurrency() {
                let block_ref = blocks[next_to_submit].clone();
                let chunk_service = self.chunk_service.clone();
                let block_service = self.block_service.clone();
                join_set.spawn(async move {
                    chunk_service.delete_chunk(&block_ref.chunk_key).await?;
                    if block_ref.block_metadata_published {
                        block_service.delete_block_ref(&block_ref.block_id).await?;
                    }
                    Ok::<_, ObjectStoreError>(())
                });
                next_to_submit += 1;
            }
            join_set
                .join_next()
                .await
                .expect("matrixobjectstore delete task missing")
                .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))??;
        }
        Ok(())
    }

    async fn delete_stale_block_refs_best_effort(
        &self,
        blocks: Vec<MatrixObjectBlockRef>,
        live_chunk_keys: &HashSet<String>,
        live_block_ids: &HashSet<String>,
    ) {
        let stale_blocks: Vec<_> = blocks
            .into_iter()
            .filter(|block| {
                !live_chunk_keys.contains(&block.chunk_key)
                    || !live_block_ids.contains(&block.block_id)
            })
            .collect();
        let _ = self.delete_block_refs(stale_blocks).await;
    }
}

#[async_trait]
impl ObjectStore for MatrixObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.put_atomic(key, bytes).await.map(|_| ())
    }

    async fn put_unique(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.put_atomic_unique(key, bytes).await.map(|_| ())
    }

    async fn put_path_unique(
        &self,
        key: &str,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put_path_unique_inner(key, path).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let manifest = self.root_service.get_manifest(key).await?;
        let mut reads = Vec::with_capacity(manifest.blocks.len());
        for (index, block_ref) in manifest.blocks.iter().cloned().enumerate() {
            reads.push(MatrixObjectChunkRead { index, block_ref });
        }
        let mut chunks = self.read_chunks(reads).await?;
        let mut out = Vec::with_capacity(manifest.size_bytes as usize);
        chunks.sort_by_key(|(index, _)| *index);
        for (_, chunk) in chunks {
            out.extend_from_slice(&chunk);
        }
        if out.len() as u64 != manifest.size_bytes || sha256_hex(&out) != manifest.checksum_sha256 {
            return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "object checksum mismatch for {key}"
            ))));
        }
        Ok(Bytes::from(out))
    }

    async fn get_to_path(
        &self,
        key: &str,
        destination: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let manifest = self.root_service.get_manifest(key).await?;
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = tokio::fs::File::create(destination).await?;
        file.set_len(manifest.size_bytes).await?;
        let mut written_bytes = 0u64;

        let mut join_set = JoinSet::new();
        let mut next_to_submit = 0usize;
        while next_to_submit < manifest.blocks.len() || !join_set.is_empty() {
            while next_to_submit < manifest.blocks.len()
                && join_set.len() < self.transfer_concurrency()
            {
                let block_ref = manifest.blocks[next_to_submit].clone();
                let chunk_service = self.chunk_service.clone();
                let block_service = self.block_service.clone();
                let verify_block_metadata = self.verify_block_metadata_on_read();
                join_set.spawn(async move {
                    let block_ref = if verify_block_metadata {
                        block_service.get_block_ref(&block_ref.block_id).await?
                    } else {
                        block_ref
                    };
                    let chunk = chunk_service.get_chunk(&block_ref.chunk_key).await?;
                    let checksum = sha256_hex(&chunk);
                    if checksum != block_ref.checksum_sha256 {
                        return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                            "chunk checksum mismatch for {}",
                            block_ref.chunk_key
                        ))));
                    }
                    Ok::<_, ObjectStoreError>((block_ref.offset, chunk))
                });
                next_to_submit += 1;
            }

            let (offset, chunk) = join_set
                .join_next()
                .await
                .expect("matrixobjectstore get_to_path task missing")
                .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))??;
            file.seek(SeekFrom::Start(offset)).await?;
            file.write_all(&chunk).await?;
            written_bytes += chunk.len() as u64;
        }
        file.flush().await?;
        drop(file);

        if written_bytes != manifest.size_bytes {
            return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "object size mismatch for {key}: expected {}, wrote {}",
                manifest.size_bytes, written_bytes
            ))));
        }
        let checksum_sha256 = sha256_file_hex(destination).await?;
        if checksum_sha256 != manifest.checksum_sha256 {
            return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "object checksum mismatch for {key}"
            ))));
        }
        Ok(ObjectMetadata {
            key: manifest.key,
            uri: manifest.uri,
            size_bytes: manifest.size_bytes,
            checksum_sha256,
        })
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
        self.delete_block_refs(manifest.blocks).await?;
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
        self.put_atomic_inner(key, bytes, true).await
    }
}

impl MatrixObjectStore {
    pub async fn put_atomic_unique(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put_atomic_inner(key, bytes, false).await
    }

    async fn put_path_unique_inner(
        &self,
        key: &str,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let (blocks, checksum_sha256, size_bytes) = self.write_chunks_from_path(key, path).await?;
        let manifest = MatrixObjectManifest {
            key: key.to_string(),
            uri: self.uri(key),
            size_bytes,
            checksum_sha256,
            created_at_ms: now_ms(),
            blocks,
        };
        if let Err(err) = self.root_service.put_manifest(&manifest).await {
            let _ = self.delete_block_refs(manifest.blocks.clone()).await;
            return Err(err);
        }
        Ok(ObjectMetadata {
            key: manifest.key,
            uri: manifest.uri,
            size_bytes: manifest.size_bytes,
            checksum_sha256: manifest.checksum_sha256,
        })
    }

    async fn put_atomic_inner(
        &self,
        key: &str,
        bytes: Bytes,
        cleanup_previous: bool,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let root_service = self.root_service.clone();
        let previous_key = key.to_string();
        let previous_manifest_task = cleanup_previous.then(|| {
            tokio::spawn(async move { root_service.get_manifest(&previous_key).await.ok() })
        });
        let checksum_sha256 = sha256_hex(&bytes);
        let blocks = self.write_chunks(key, bytes.clone()).await?;
        let previous_manifest = match previous_manifest_task {
            Some(task) => task
                .await
                .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))?,
            None => None,
        };
        let live_chunk_keys: HashSet<String> =
            blocks.iter().map(|block| block.chunk_key.clone()).collect();
        let live_block_ids: HashSet<String> =
            blocks.iter().map(|block| block.block_id.clone()).collect();
        let manifest = MatrixObjectManifest {
            key: key.to_string(),
            uri: self.uri(key),
            size_bytes: bytes.len() as u64,
            checksum_sha256,
            created_at_ms: now_ms(),
            blocks,
        };
        if let Err(err) = self.root_service.put_manifest(&manifest).await {
            let _ = self.delete_block_refs(manifest.blocks.clone()).await;
            return Err(err);
        }
        if let Some(previous_manifest) = previous_manifest {
            self.delete_stale_block_refs_best_effort(
                previous_manifest.blocks,
                &live_chunk_keys,
                &live_block_ids,
            )
            .await;
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

fn default_block_metadata_published() -> bool {
    true
}

fn object_key_fingerprint(object_key: &str) -> String {
    sha256_hex(object_key.as_bytes()).chars().take(16).collect()
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

fn default_matrixobjectstore_chunk_target_bytes() -> usize {
    std::env::var("TS_MATRIXOBJECTSTORE_CHUNK_TARGET_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(8 * 1024 * 1024)
}

fn default_matrixobjectstore_transfer_concurrency() -> usize {
    std::env::var("TS_MATRIXOBJECTSTORE_TRANSFER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4)
}

fn default_strict_block_metadata() -> bool {
    env_bool("TS_MATRIXOBJECTSTORE_VERIFY_BLOCK_METADATA_ON_READ", false)
}

fn default_publish_block_metadata() -> bool {
    env_bool("TS_MATRIXOBJECTSTORE_PUBLISH_BLOCK_METADATA", false)
}

fn default_matrixobjectstore_sync_writes() -> bool {
    env_bool("TS_MATRIXOBJECTSTORE_SYNC_WRITES", true)
}

fn default_matrixobjectstore_sync_parent_dirs() -> bool {
    env_bool("TS_MATRIXOBJECTSTORE_SYNC_PARENT_DIRS", true)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

async fn sync_parent_dir(path: &Path) -> Result<(), ObjectStoreError> {
    if let Some(parent) = path.parent() {
        sync_dir(parent).await?;
    }
    Ok(())
}

async fn sync_directory_chain(root: &Path, dir: &Path) -> Result<(), ObjectStoreError> {
    let mut current = root.to_path_buf();
    sync_dir(&current).await?;
    if let Ok(relative) = dir.strip_prefix(root) {
        for component in relative.components() {
            current.push(component);
            sync_dir(&current).await?;
        }
    } else {
        sync_dir(dir).await?;
    }
    Ok(())
}

#[cfg(unix)]
async fn sync_dir(path: &Path) -> Result<(), ObjectStoreError> {
    let dir = tokio::fs::File::open(path).await?;
    dir.sync_all().await?;
    Ok(())
}

#[cfg(not(unix))]
async fn sync_dir(_path: &Path) -> Result<(), ObjectStoreError> {
    Ok(())
}

async fn write_object_file(path: &Path, bytes: &Bytes) -> Result<(), ObjectStoreError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::File::create(path).await?;
    file.write_all(bytes).await?;
    file.flush().await?;
    Ok(())
}

async fn sha256_file_hex(path: &Path) -> Result<String, ObjectStoreError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

#[async_trait]
impl ObjectStore for SharedObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.put(key, bytes).await,
            Self::MatrixObjectStore(store) => store.put(key, bytes).await,
        }
    }

    async fn put_unique(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.put_unique(key, bytes).await,
            Self::MatrixObjectStore(store) => store.put_unique(key, bytes).await,
        }
    }

    async fn put_path_unique(
        &self,
        key: &str,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => {
                store.put_path_unique(key, path).await
            }
            Self::MatrixObjectStore(store) => store.put_path_unique(key, path).await,
        }
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.get(key).await,
            Self::MatrixObjectStore(store) => store.get(key).await,
        }
    }

    async fn get_to_path(
        &self,
        key: &str,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.get_to_path(key, path).await,
            Self::MatrixObjectStore(store) => store.get_to_path(key, path).await,
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
    collect_files_with_suffix(root, dir, "", out).await
}

async fn collect_files_with_suffix(
    root: &Path,
    dir: &Path,
    suffix: &str,
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
                let key = rel.to_string_lossy().replace('\\', "/");
                if suffix.is_empty() || key.ends_with(suffix) {
                    out.push(key);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        FileObjectStore, MatrixObjectStore, MatrixObjectStoreBackendMode, MatrixObjectStoreConfig,
        MatrixObjectStoreServiceEndpoints, ObjectStore, ObjectStoreError, SharedObjectStore,
        SharedObjectStoreBackend, SharedObjectStoreConfig,
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
    async fn matrix_object_store_overwrite_removes_stale_chunked_refs() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2);
        let store = MatrixObjectStore::from_config(config);

        store
            .put_atomic(
                "large/object",
                Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz"),
            )
            .await
            .unwrap();
        let old_manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();
        store
            .put_atomic("large/object", Bytes::from_static(b"1234567890"))
            .await
            .unwrap();
        let new_manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();
        let new_chunk_keys: std::collections::HashSet<_> = new_manifest
            .blocks
            .iter()
            .map(|block| block.chunk_key.as_str())
            .collect();
        let new_block_ids: std::collections::HashSet<_> = new_manifest
            .blocks
            .iter()
            .map(|block| block.block_id.as_str())
            .collect();

        for block in old_manifest.blocks {
            if !new_chunk_keys.contains(block.chunk_key.as_str()) {
                assert!(matches!(
                    store.chunk_service.get_chunk(&block.chunk_key).await,
                    Err(ObjectStoreError::NotFound(_))
                ));
            }
            if !new_block_ids.contains(block.block_id.as_str()) {
                assert!(matches!(
                    store.block_service.get_block_ref(&block.block_id).await,
                    Err(ObjectStoreError::NotFound(_))
                ));
            }
        }
    }

    #[tokio::test]
    async fn matrix_object_store_splits_large_objects_into_chunk_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2);
        let store = MatrixObjectStore::from_config(config);
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        let metadata = store
            .put_atomic("large/object", payload.clone())
            .await
            .unwrap();
        let manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();

        assert_eq!(metadata.size_bytes, payload.len() as u64);
        assert!(manifest.blocks.len() > 1);
        assert_eq!(manifest.blocks[0].offset, 0);
        assert_eq!(store.get("large/object").await.unwrap(), payload);
    }

    #[tokio::test]
    async fn matrix_object_store_downloads_chunked_object_directly_to_path() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2);
        let store = MatrixObjectStore::from_config(config);
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        let put_metadata = store
            .put_atomic("large/object", payload.clone())
            .await
            .unwrap();
        let destination = dir.path().join("restore/large-object.bin");
        let get_metadata = store
            .get_to_path("large/object", &destination)
            .await
            .unwrap();

        assert_eq!(get_metadata, put_metadata);
        assert_eq!(tokio::fs::read(destination).await.unwrap(), payload);
    }

    #[tokio::test]
    async fn matrix_object_store_uploads_file_as_chunked_object() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2);
        let store = MatrixObjectStore::from_config(config);
        let source = dir.path().join("source/large-object.bin");
        tokio::fs::create_dir_all(source.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&source, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .unwrap();

        let metadata = store
            .put_path_unique("large/object", &source)
            .await
            .unwrap();
        let manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();

        assert_eq!(metadata.size_bytes, 26);
        assert_eq!(manifest.blocks.len(), 6);
        assert_eq!(
            store.get("large/object").await.unwrap(),
            Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz")
        );
    }

    #[tokio::test]
    async fn matrix_object_store_unique_put_writes_chunked_payload() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2);
        let store = MatrixObjectStore::from_config(config);
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        store
            .put_unique(
                "shared/oplog/oplog_00000000000000000001.json",
                payload.clone(),
            )
            .await
            .unwrap();

        let manifest = store
            .root_service
            .get_manifest("shared/oplog/oplog_00000000000000000001.json")
            .await
            .unwrap();
        assert!(manifest.blocks.len() > 1);
        assert_eq!(
            store
                .get("shared/oplog/oplog_00000000000000000001.json")
                .await
                .unwrap(),
            payload
        );
    }

    #[tokio::test]
    async fn matrix_object_store_delete_removes_chunked_payload_refs() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2);
        let store = MatrixObjectStore::from_config(config);
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        store.put_atomic("large/object", payload).await.unwrap();
        let manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();
        assert!(manifest.blocks.len() > 1);
        store.delete("large/object").await.unwrap();

        assert!(matches!(
            store.root_service.get_manifest("large/object").await,
            Err(ObjectStoreError::NotFound(_))
        ));
        for block in manifest.blocks {
            assert!(matches!(
                store.chunk_service.get_chunk(&block.chunk_key).await,
                Err(ObjectStoreError::NotFound(_))
            ));
            assert!(matches!(
                store.block_service.get_block_ref(&block.block_id).await,
                Err(ObjectStoreError::NotFound(_))
            ));
        }
    }

    #[tokio::test]
    async fn matrix_object_store_normal_read_uses_manifest_block_refs() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2);
        let store = MatrixObjectStore::from_config(config);
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        store
            .put_atomic("large/object", payload.clone())
            .await
            .unwrap();
        let manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();
        for block in &manifest.blocks {
            store
                .block_service
                .delete_block_ref(&block.block_id)
                .await
                .unwrap();
        }

        assert_eq!(store.get("large/object").await.unwrap(), payload);
    }

    #[tokio::test]
    async fn matrix_object_store_default_write_skips_block_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2);
        let store = MatrixObjectStore::from_config(config);
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        store
            .put_atomic("large/object", payload.clone())
            .await
            .unwrap();
        let manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();

        assert!(manifest.blocks.len() > 1);
        assert!(!manifest.blocks[0].block_metadata_published);
        assert!(matches!(
            store
                .block_service
                .get_block_ref(&manifest.blocks[0].block_id)
                .await,
            Err(ObjectStoreError::NotFound(_))
        ));
        assert_eq!(store.get("large/object").await.unwrap(), payload);
    }

    #[tokio::test]
    async fn matrix_object_store_strict_read_verifies_block_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2)
            .with_verify_block_metadata_on_read(true);
        let store = MatrixObjectStore::from_config(config);
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        store.put_atomic("large/object", payload).await.unwrap();
        let manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();
        assert!(manifest.blocks[0].block_metadata_published);
        store
            .block_service
            .get_block_ref(&manifest.blocks[0].block_id)
            .await
            .unwrap();
        store
            .block_service
            .delete_block_ref(&manifest.blocks[0].block_id)
            .await
            .unwrap();

        assert!(matches!(
            store.get("large/object").await,
            Err(ObjectStoreError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn matrix_object_store_block_ids_are_object_scoped() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_chunk_target_bytes(5)
            .with_transfer_concurrency(2)
            .with_verify_block_metadata_on_read(true);
        let store = MatrixObjectStore::from_config(config);
        let payload = Bytes::from_static(b"same-payload-same-offsets");

        store.put_atomic("object/a", payload.clone()).await.unwrap();
        store.put_atomic("object/b", payload.clone()).await.unwrap();
        let manifest_a = store.root_service.get_manifest("object/a").await.unwrap();
        let manifest_b = store.root_service.get_manifest("object/b").await.unwrap();

        assert_ne!(manifest_a.blocks[0].block_id, manifest_b.blocks[0].block_id);
        store.delete("object/a").await.unwrap();
        assert_eq!(store.get("object/b").await.unwrap(), payload);
    }

    #[tokio::test]
    async fn matrix_object_store_keeps_explicit_backend_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::external("matrixobjectstore://cluster-a", dir.path())
            .with_uri_scheme("matrixobjectstore")
            .with_sync_writes(false);
        let store = MatrixObjectStore::from_config(config.clone());

        assert_eq!(store.config(), &config);
        assert!(!store.config().sync_writes);
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
    async fn matrix_object_store_threads_sync_write_policy_to_split_services() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::local_compat(dir.path())
            .with_sync_writes(false)
            .with_sync_parent_dirs(false);
        let store = MatrixObjectStore::from_config(config);

        assert!(!store.config().sync_writes);
        assert!(!store.config().sync_parent_dirs);
        assert!(!store.root_service.manifest_store.sync_writes);
        assert!(!store.root_service.manifest_store.sync_parent_dirs);
        assert!(!store.block_service.block_store.sync_writes);
        assert!(!store.block_service.block_store.sync_parent_dirs);
        assert!(!store.chunk_service.chunk_store.sync_writes);
        assert!(!store.chunk_service.chunk_store.sync_parent_dirs);

        store
            .put_atomic(
                "fast/local-object",
                Bytes::from_static(b"fast shared-store payload"),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get("fast/local-object").await.unwrap(),
            Bytes::from_static(b"fast shared-store payload")
        );
    }

    #[tokio::test]
    async fn matrix_object_store_supports_split_service_endpoints() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::external("matrixobjectstore://cluster-a", dir.path())
            .with_service_endpoints(MatrixObjectStoreServiceEndpoints::split(
                "matrixobjectstore-root://root-1",
                "matrixobjectstore-block://block-1",
                "matrixobjectstore-chunk://chunk-1",
            ));
        let store = MatrixObjectStore::from_config(config.clone());
        let topology = store.service_topology();

        assert_eq!(
            topology.backend_mode,
            MatrixObjectStoreBackendMode::External
        );
        assert_eq!(topology.root_service.service_role, "root");
        assert_eq!(
            topology.root_service.endpoint.as_deref(),
            Some("matrixobjectstore-root://root-1")
        );
        assert_eq!(
            topology.block_service.endpoint.as_deref(),
            Some("matrixobjectstore-block://block-1")
        );
        assert_eq!(
            topology.chunk_service.endpoint.as_deref(),
            Some("matrixobjectstore-chunk://chunk-1")
        );

        store
            .put_atomic("split/object", Bytes::from_static(b"split payload"))
            .await
            .unwrap();
        assert_eq!(
            store.get("split/object").await.unwrap(),
            Bytes::from_static(b"split payload")
        );
        assert_eq!(store.config(), &config);
    }

    #[tokio::test]
    async fn matrix_object_store_list_only_materializes_manifests() {
        let dir = tempfile::tempdir().unwrap();
        let store = MatrixObjectStore::new(dir.path());

        store
            .put_atomic("raw/object-a", Bytes::from_static(b"payload-a"))
            .await
            .unwrap();
        let stray = store
            .root_service
            .manifest_store
            .root
            .join("raw/not-a-manifest.tmp");
        tokio::fs::write(&stray, b"ignore me").await.unwrap();

        assert_eq!(
            store.list("raw/").await.unwrap(),
            vec!["raw/object-a".to_string()]
        );
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
    async fn file_object_store_uploads_file_without_buffering_payload() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileObjectStore::new(dir.path());
        let source = dir.path().join("source.bin");
        tokio::fs::write(&source, b"file-object-store-payload")
            .await
            .unwrap();

        let metadata = store
            .put_path_unique("objects/payload.bin", &source)
            .await
            .unwrap();

        assert_eq!(metadata.key, "objects/payload.bin");
        assert_eq!(metadata.size_bytes, 25);
        assert_eq!(
            store.get("objects/payload.bin").await.unwrap(),
            Bytes::from_static(b"file-object-store-payload")
        );
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
