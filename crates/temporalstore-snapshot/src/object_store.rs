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
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("object already exists: {0}")]
    AlreadyExists(String),
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectListPage {
    pub keys: Vec<String>,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectStoreCapabilities {
    pub backend: String,
    pub uri_scheme: String,
    pub runtime_linked: bool,
    pub operations_fail_closed: bool,
    pub atomic_put: bool,
    pub unique_put: bool,
    pub conditional_create: bool,
    pub direct_upload_from_path: bool,
    pub direct_download_to_path: bool,
    pub metadata_head: bool,
    pub prefix_list: bool,
    pub paginated_list: bool,
    pub delete: bool,
    pub bulk_delete: bool,
    pub copy_object: bool,
    pub delete_prefix: bool,
    pub byte_range_read: bool,
    pub checksum_sha256: bool,
    pub object_etag: bool,
    pub object_version_id: bool,
    pub split_services: bool,
}

impl ObjectStoreCapabilities {
    pub fn file(uri_scheme: impl Into<String>) -> Self {
        let uri_scheme = uri_scheme.into();
        Self {
            backend: if uri_scheme == "shared-file" {
                "shared_file".to_string()
            } else {
                "local_file".to_string()
            },
            uri_scheme,
            runtime_linked: true,
            operations_fail_closed: false,
            atomic_put: true,
            unique_put: true,
            conditional_create: true,
            direct_upload_from_path: true,
            direct_download_to_path: true,
            metadata_head: true,
            prefix_list: true,
            paginated_list: true,
            delete: true,
            bulk_delete: true,
            copy_object: true,
            delete_prefix: true,
            byte_range_read: true,
            checksum_sha256: true,
            object_etag: true,
            object_version_id: false,
            split_services: false,
        }
    }

    pub fn matrixobject(uri_scheme: impl Into<String>, split_services: bool) -> Self {
        Self {
            backend: "matrixobject".to_string(),
            uri_scheme: uri_scheme.into(),
            runtime_linked: true,
            operations_fail_closed: false,
            atomic_put: true,
            unique_put: true,
            conditional_create: true,
            direct_upload_from_path: true,
            direct_download_to_path: true,
            metadata_head: true,
            prefix_list: true,
            paginated_list: true,
            delete: true,
            bulk_delete: true,
            copy_object: true,
            delete_prefix: true,
            byte_range_read: true,
            checksum_sha256: true,
            object_etag: true,
            object_version_id: false,
            split_services,
        }
    }

    pub fn unsupported(backend: SharedObjectStoreBackend) -> Self {
        Self {
            backend: backend.canonical_name().to_string(),
            uri_scheme: backend.uri_scheme().to_string(),
            runtime_linked: false,
            operations_fail_closed: true,
            atomic_put: false,
            unique_put: false,
            conditional_create: false,
            direct_upload_from_path: false,
            direct_download_to_path: false,
            metadata_head: false,
            prefix_list: false,
            paginated_list: false,
            delete: false,
            bulk_delete: false,
            copy_object: false,
            delete_prefix: false,
            byte_range_read: false,
            checksum_sha256: false,
            object_etag: false,
            object_version_id: false,
            split_services: false,
        }
    }

    pub fn remote_expected(backend: SharedObjectStoreBackend) -> Self {
        let supports_object_api = matches!(
            backend,
            SharedObjectStoreBackend::S3
                | SharedObjectStoreBackend::CephS3
                | SharedObjectStoreBackend::CephRados
        );
        Self {
            backend: backend.canonical_name().to_string(),
            uri_scheme: backend.uri_scheme().to_string(),
            runtime_linked: false,
            operations_fail_closed: true,
            atomic_put: supports_object_api,
            unique_put: supports_object_api,
            conditional_create: supports_object_api,
            direct_upload_from_path: supports_object_api,
            direct_download_to_path: supports_object_api,
            metadata_head: supports_object_api,
            prefix_list: supports_object_api,
            paginated_list: supports_object_api,
            delete: supports_object_api,
            bulk_delete: supports_object_api,
            copy_object: supports_object_api,
            delete_prefix: supports_object_api,
            byte_range_read: supports_object_api,
            checksum_sha256: supports_object_api,
            object_etag: supports_object_api,
            object_version_id: supports_object_api,
            split_services: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectStoreServiceDescriptor {
    pub role: String,
    pub endpoint: Option<String>,
    pub local_root: Option<PathBuf>,
}

impl ObjectStoreServiceDescriptor {
    pub fn local(role: impl Into<String>, local_root: impl Into<PathBuf>) -> Self {
        Self {
            role: role.into(),
            endpoint: None,
            local_root: Some(local_root.into()),
        }
    }

    pub fn endpoint(role: impl Into<String>, endpoint: Option<String>) -> Self {
        Self {
            role: role.into(),
            endpoint,
            local_root: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectStoreTopology {
    pub backend: String,
    pub uri_scheme: String,
    pub namespace: Option<String>,
    pub key_prefix: Option<String>,
    pub services: Vec<ObjectStoreServiceDescriptor>,
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
            "matrixobject"
            | "matrix_object"
            | "matrixobject_local_compat"
            | "matrixobjectstore"
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
            Self::MatrixObjectStore => "matrixobject",
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
            Self::MatrixObjectStore => "matrixobject",
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

fn parse_remote_namespace_and_prefix(uri: &str) -> (Option<String>, Option<String>) {
    let Some((_, remainder)) = uri.split_once("://") else {
        return (None, None);
    };
    let remainder = remainder.trim_matches('/');
    if remainder.is_empty() {
        return (None, None);
    }
    let (namespace, prefix) = remainder
        .split_once('/')
        .map_or((remainder, ""), |(namespace, prefix)| (namespace, prefix));
    let namespace = (!namespace.is_empty()).then(|| namespace.to_string());
    let prefix = prefix.trim_matches('/');
    let key_prefix = (!prefix.is_empty()).then(|| prefix.to_string());
    (namespace, key_prefix)
}

fn validate_object_key(key: &str) -> Result<(), ObjectStoreError> {
    if key.is_empty()
        || key.starts_with('/')
        || key.contains('\\')
        || key.split('/').any(|part| part == "." || part == "..")
    {
        return Err(ObjectStoreError::InvalidKey(key.to_string()));
    }
    Ok(())
}

fn validate_object_prefix(prefix: &str) -> Result<(), ObjectStoreError> {
    let prefix = prefix.trim_start_matches('/');
    if prefix.contains('\\') || prefix.split('/').any(|part| part == "." || part == "..") {
        return Err(ObjectStoreError::InvalidKey(prefix.to_string()));
    }
    Ok(())
}

fn percent_encode_path(value: &str) -> String {
    value
        .split('/')
        .map(percent_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn percent_encode_path_segment(value: &str) -> String {
    percent_encode(value, false)
}

fn percent_encode_query_value(value: &str) -> String {
    percent_encode(value, true)
}

fn percent_encode(value: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (!encode_slash && byte == b'/');
        if keep {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn parse_http_url(url: &str) -> Result<(String, u16, String), ObjectStoreError> {
    let Some(remainder) = url.strip_prefix("http://") else {
        return Err(ObjectStoreError::UnsupportedBackend {
            backend: "http".to_string(),
            uri: url.to_string(),
        });
    };
    let (authority, path) = remainder
        .split_once('/')
        .map_or((remainder, "/"), |(authority, path)| (authority, path));
    if authority.is_empty() {
        return Err(ObjectStoreError::InvalidKey(format!(
            "remote endpoint has no host: {url}"
        )));
    }
    let (host, port) = authority
        .rsplit_once(':')
        .and_then(|(host, port)| port.parse::<u16>().ok().map(|port| (host, port)))
        .map_or((authority, 80), |(host, port)| (host, port));
    if host.is_empty() {
        return Err(ObjectStoreError::InvalidKey(format!(
            "remote endpoint has no host: {url}"
        )));
    }
    Ok((host.to_string(), port, format!("/{path}")))
}

fn host_header(host: &str, port: u16) -> String {
    if port == 80 {
        host.to_string()
    } else {
        format!("{host}:{port}")
    }
}

fn parse_http_response(raw: Vec<u8>) -> Result<RemoteHttpResponse, ObjectStoreError> {
    let header_end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| ObjectStoreError::Io(std::io::Error::other("invalid HTTP response")))?;
    let header_bytes = &raw[..header_end];
    let body = Bytes::from(raw[(header_end + 4)..].to_vec());
    let header_text = String::from_utf8_lossy(header_bytes);
    let mut lines = header_text.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| ObjectStoreError::Io(std::io::Error::other("missing HTTP status")))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| ObjectStoreError::Io(std::io::Error::other("invalid HTTP status")))?;
    let headers = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    Ok(RemoteHttpResponse {
        status,
        headers,
        body,
    })
}

fn http_header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    let name = name.to_ascii_lowercase();
    headers
        .iter()
        .find(|(header, _)| header == &name)
        .map(|(_, value)| value.as_str())
}

fn parse_s3_list_keys(xml: &str) -> Vec<String> {
    parse_xml_tag_values(xml, "Key")
}

fn parse_s3_next_continuation_token(xml: &str) -> Option<String> {
    parse_xml_tag_values(xml, "NextContinuationToken")
        .into_iter()
        .next()
}

fn parse_xml_tag_values(xml: &str, tag: &str) -> Vec<String> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let mut values = Vec::new();
    let mut remainder = xml;
    while let Some((_, after_start)) = remainder.split_once(&start_tag) {
        let Some((value, after_end)) = after_start.split_once(&end_tag) else {
            break;
        };
        values.push(xml_unescape(value));
        remainder = after_end;
    }
    values
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn map_http_status(
    status: u16,
    key: &str,
    ok: impl FnOnce() -> Result<(), ObjectStoreError>,
) -> Result<(), ObjectStoreError> {
    match status {
        200..=299 => ok(),
        404 => Err(ObjectStoreError::NotFound(key.to_string())),
        409 | 412 => Err(ObjectStoreError::AlreadyExists(key.to_string())),
        _ => Err(ObjectStoreError::Io(std::io::Error::other(format!(
            "remote object HTTP status {status} for {key}"
        )))),
    }
}

impl ObjectMetadata {
    pub fn from_parts(
        key: impl Into<String>,
        uri: impl Into<String>,
        size_bytes: u64,
        checksum_sha256: impl Into<String>,
    ) -> Self {
        let checksum_sha256 = checksum_sha256.into();
        let etag = (!checksum_sha256.is_empty()).then(|| format!("sha256:{checksum_sha256}"));
        Self {
            key: key.into(),
            uri: uri.into(),
            size_bytes,
            checksum_sha256,
            etag,
            version_id: None,
        }
    }

    pub fn from_bytes(key: &str, uri: String, bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self::from_parts(key, uri, bytes.len() as u64, hex::encode(hasher.finalize()))
    }
}

fn list_page_from_sorted_keys(
    keys: &[String],
    continuation_token: Option<&str>,
    max_keys: usize,
) -> ObjectListPage {
    let start = continuation_token
        .and_then(|token| keys.iter().position(|key| key.as_str() > token))
        .unwrap_or_else(|| {
            if continuation_token.is_some() {
                keys.len()
            } else {
                0
            }
        });
    if max_keys == 0 || start >= keys.len() {
        return ObjectListPage {
            keys: Vec::new(),
            next_continuation_token: None,
        };
    }
    let end = start.saturating_add(max_keys).min(keys.len());
    let page_keys = keys[start..end].to_vec();
    let next_continuation_token = if end < keys.len() {
        page_keys.last().cloned()
    } else {
        None
    };
    ObjectListPage {
        keys: page_keys,
        next_continuation_token,
    }
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError>;
    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        match self.head(key).await {
            Ok(_) => Err(ObjectStoreError::AlreadyExists(key.to_string())),
            Err(ObjectStoreError::NotFound(_)) => self.put_atomic(key, bytes).await,
            Err(err) => Err(err),
        }
    }
    async fn put_unique(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.put_if_absent(key, bytes).await.map(|_| ())
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
    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ObjectStoreError> {
        let bytes = self.get(key).await?;
        let start = usize::try_from(offset).map_err(|_| {
            ObjectStoreError::Io(std::io::Error::other(format!(
                "range offset too large for object {key}: {offset}"
            )))
        })?;
        if start >= bytes.len() || length == 0 {
            return Ok(Bytes::new());
        }
        let end = start.saturating_add(length).min(bytes.len());
        Ok(bytes.slice(start..end))
    }
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
    async fn list_page(
        &self,
        prefix: &str,
        continuation_token: Option<&str>,
        max_keys: usize,
    ) -> Result<ObjectListPage, ObjectStoreError> {
        let mut keys = self.list(prefix).await?;
        keys.sort();
        Ok(list_page_from_sorted_keys(
            &keys,
            continuation_token,
            max_keys,
        ))
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;
    async fn delete_objects(&self, keys: &[String]) -> Result<usize, ObjectStoreError> {
        let mut deleted = 0usize;
        for key in keys {
            self.delete(key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }
    async fn copy_object(
        &self,
        source_key: &str,
        destination_key: &str,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let bytes = self.get(source_key).await?;
        self.put_atomic(destination_key, bytes).await
    }
    async fn delete_prefix(&self, prefix: &str) -> Result<usize, ObjectStoreError> {
        let mut deleted = 0usize;
        let mut continuation_token = None;
        loop {
            let page = self
                .list_page(prefix, continuation_token.as_deref(), 1024)
                .await?;
            if page.keys.is_empty() {
                break;
            }
            deleted += self.delete_objects(&page.keys).await?;
            continuation_token = page.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(deleted)
    }
    fn uri(&self, key: &str) -> String;
    fn capabilities(&self) -> ObjectStoreCapabilities {
        ObjectStoreCapabilities {
            backend: "custom".to_string(),
            uri_scheme: "custom".to_string(),
            runtime_linked: true,
            operations_fail_closed: false,
            atomic_put: true,
            unique_put: true,
            conditional_create: true,
            direct_upload_from_path: false,
            direct_download_to_path: false,
            metadata_head: false,
            prefix_list: true,
            paginated_list: true,
            delete: true,
            bulk_delete: true,
            copy_object: true,
            delete_prefix: true,
            byte_range_read: false,
            checksum_sha256: false,
            object_etag: false,
            object_version_id: false,
            split_services: false,
        }
    }
    fn topology(&self) -> ObjectStoreTopology {
        let capabilities = self.capabilities();
        ObjectStoreTopology {
            backend: capabilities.backend,
            uri_scheme: capabilities.uri_scheme,
            namespace: None,
            key_prefix: None,
            services: vec![ObjectStoreServiceDescriptor::endpoint("object", None)],
        }
    }

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
            uri_scheme: "matrixobject".to_string(),
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
            uri_scheme: "matrixobject".to_string(),
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
pub struct RemoteObjectStore {
    backend: SharedObjectStoreBackend,
    uri: String,
    endpoint: Option<String>,
    namespace: Option<String>,
    key_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteObjectRequestPlan {
    pub backend: String,
    pub method: String,
    pub url: String,
    pub object_key: Option<String>,
    pub copy_source: Option<String>,
}

struct RemoteHttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
}

impl RemoteObjectStore {
    pub fn new(
        backend: SharedObjectStoreBackend,
        uri: impl Into<String>,
        endpoint: Option<String>,
    ) -> Self {
        let uri = uri.into();
        let (namespace, key_prefix) = parse_remote_namespace_and_prefix(&uri);
        Self {
            backend,
            uri,
            endpoint,
            namespace,
            key_prefix,
        }
    }

    fn unsupported<T>(&self) -> Result<T, ObjectStoreError> {
        Err(ObjectStoreError::UnsupportedBackend {
            backend: self.backend.canonical_name().to_string(),
            uri: self.uri.clone(),
        })
    }

    fn supports_unsigned_http(&self) -> bool {
        matches!(
            self.backend,
            SharedObjectStoreBackend::S3 | SharedObjectStoreBackend::CephS3
        ) && self
            .endpoint
            .as_deref()
            .is_some_and(|endpoint| endpoint.starts_with("http://"))
    }

    fn require_unsigned_http(&self) -> Result<(), ObjectStoreError> {
        if self.supports_unsigned_http() {
            Ok(())
        } else {
            self.unsupported()
        }
    }

    fn endpoint_base(&self) -> Result<String, ObjectStoreError> {
        self.endpoint
            .as_ref()
            .map(|endpoint| endpoint.trim_end_matches('/').to_string())
            .ok_or_else(|| ObjectStoreError::UnsupportedBackend {
                backend: self.backend.canonical_name().to_string(),
                uri: self.uri.clone(),
            })
    }

    fn namespace(&self) -> Result<&str, ObjectStoreError> {
        self.namespace.as_deref().ok_or_else(|| {
            ObjectStoreError::InvalidKey(format!(
                "remote object URI has no namespace: {}",
                self.uri
            ))
        })
    }

    fn remote_object_key(&self, key: &str) -> Result<String, ObjectStoreError> {
        let key = key.trim_start_matches('/');
        if key.is_empty() {
            return Err(ObjectStoreError::InvalidKey(
                "remote object key is empty".to_string(),
            ));
        }
        validate_object_key(key)?;
        Ok(self
            .key_prefix
            .as_deref()
            .filter(|prefix| !prefix.is_empty())
            .map_or_else(|| key.to_string(), |prefix| format!("{prefix}/{key}")))
    }

    fn object_url(&self, object_key: &str) -> Result<String, ObjectStoreError> {
        let endpoint = self.endpoint_base()?;
        let namespace = self.namespace()?;
        Ok(format!(
            "{}/{}/{}",
            endpoint,
            percent_encode_path_segment(namespace),
            percent_encode_path(object_key)
        ))
    }

    pub fn plan_put(&self, key: &str) -> Result<RemoteObjectRequestPlan, ObjectStoreError> {
        self.plan_object_request("PUT", key, None)
    }

    pub fn plan_get(&self, key: &str) -> Result<RemoteObjectRequestPlan, ObjectStoreError> {
        self.plan_object_request("GET", key, None)
    }

    pub fn plan_head(&self, key: &str) -> Result<RemoteObjectRequestPlan, ObjectStoreError> {
        self.plan_object_request("HEAD", key, None)
    }

    pub fn plan_delete(&self, key: &str) -> Result<RemoteObjectRequestPlan, ObjectStoreError> {
        self.plan_object_request("DELETE", key, None)
    }

    pub fn plan_copy(
        &self,
        source_key: &str,
        destination_key: &str,
    ) -> Result<RemoteObjectRequestPlan, ObjectStoreError> {
        let source_object_key = self.remote_object_key(source_key)?;
        self.plan_object_request(
            "PUT",
            destination_key,
            Some(format!(
                "/{}/{}",
                percent_encode_path_segment(self.namespace()?),
                percent_encode_path(&source_object_key)
            )),
        )
    }

    pub fn plan_list(
        &self,
        prefix: &str,
        continuation_token: Option<&str>,
        max_keys: usize,
    ) -> Result<RemoteObjectRequestPlan, ObjectStoreError> {
        let endpoint = self.endpoint_base()?;
        let namespace = self.namespace()?;
        validate_object_prefix(prefix)?;
        let object_prefix = self
            .key_prefix
            .as_deref()
            .filter(|prefix| !prefix.is_empty())
            .map_or_else(
                || prefix.trim_start_matches('/').to_string(),
                |key_prefix| {
                    let prefix = prefix.trim_start_matches('/');
                    if prefix.is_empty() {
                        key_prefix.to_string()
                    } else {
                        format!("{key_prefix}/{prefix}")
                    }
                },
            );
        let mut url = format!(
            "{}/{}?list-type=2&prefix={}&max-keys={}",
            endpoint,
            percent_encode_path_segment(namespace),
            percent_encode_query_value(&object_prefix),
            max_keys
        );
        if let Some(token) = continuation_token.filter(|token| !token.is_empty()) {
            url.push_str("&continuation-token=");
            url.push_str(&percent_encode_query_value(token));
        }
        Ok(RemoteObjectRequestPlan {
            backend: self.backend.canonical_name().to_string(),
            method: "GET".to_string(),
            url,
            object_key: None,
            copy_source: None,
        })
    }

    fn plan_object_request(
        &self,
        method: &str,
        key: &str,
        copy_source: Option<String>,
    ) -> Result<RemoteObjectRequestPlan, ObjectStoreError> {
        let object_key = self.remote_object_key(key)?;
        Ok(RemoteObjectRequestPlan {
            backend: self.backend.canonical_name().to_string(),
            method: method.to_string(),
            url: self.object_url(&object_key)?,
            object_key: Some(object_key),
            copy_source,
        })
    }

    async fn http_request(
        &self,
        plan: RemoteObjectRequestPlan,
        extra_headers: Vec<(String, String)>,
        body: Bytes,
    ) -> Result<RemoteHttpResponse, ObjectStoreError> {
        self.require_unsigned_http()?;
        let (host, port, path) = parse_http_url(&plan.url)?;
        let mut stream = TcpStream::connect((host.as_str(), port)).await?;
        let mut request = format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n",
            plan.method,
            path,
            host_header(&host, port),
            body.len()
        );
        if let Some(copy_source) = plan.copy_source {
            request.push_str("x-amz-copy-source: ");
            request.push_str(&copy_source);
            request.push_str("\r\n");
        }
        for (name, value) in extra_headers {
            request.push_str(&name);
            request.push_str(": ");
            request.push_str(&value);
            request.push_str("\r\n");
        }
        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await?;
        if !body.is_empty() {
            stream.write_all(&body).await?;
        }
        stream.flush().await?;
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await?;
        parse_http_response(raw)
    }

    async fn http_metadata_from_get(&self, key: &str) -> Result<ObjectMetadata, ObjectStoreError> {
        let bytes = self.get(key).await?;
        Ok(ObjectMetadata::from_bytes(key, self.uri(key), &bytes))
    }

    fn public_key_from_remote(&self, object_key: &str) -> Option<String> {
        self.key_prefix
            .as_deref()
            .filter(|prefix| !prefix.is_empty())
            .map_or_else(
                || Some(object_key.to_string()),
                |prefix| {
                    object_key
                        .strip_prefix(prefix)
                        .and_then(|key| key.strip_prefix('/'))
                        .map(ToString::to_string)
                },
            )
    }
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

impl MatrixObjectStoreServiceDescriptor {
    fn as_generic(&self) -> ObjectStoreServiceDescriptor {
        ObjectStoreServiceDescriptor {
            role: self.service_role.clone(),
            endpoint: self.endpoint.clone(),
            local_root: Some(self.local_root.clone()),
        }
    }
}

impl MatrixObjectStoreServiceTopology {
    pub fn as_generic(&self, uri_scheme: impl Into<String>) -> ObjectStoreTopology {
        ObjectStoreTopology {
            backend: "matrixobject".to_string(),
            uri_scheme: uri_scheme.into(),
            namespace: None,
            key_prefix: None,
            services: vec![
                self.root_service.as_generic(),
                self.block_service.as_generic(),
                self.chunk_service.as_generic(),
            ],
        }
    }
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
    Remote(RemoteObjectStore),
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
            | SharedObjectStoreBackend::CephRados => Ok(Self::Remote(RemoteObjectStore::new(
                config.backend,
                config.uri,
                config.endpoint,
            ))),
            SharedObjectStoreBackend::Unknown => Err(ObjectStoreError::UnsupportedBackend {
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
                "matrixobject-root",
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

    async fn list_manifest_keys_page(
        &self,
        prefix: &str,
        continuation_token: Option<&str>,
        max_keys: usize,
    ) -> Result<ObjectListPage, ObjectStoreError> {
        let keys = self.list_manifest_keys(prefix).await?;
        Ok(list_page_from_sorted_keys(
            &keys,
            continuation_token,
            max_keys,
        ))
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
                "matrixobject-block",
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
                "matrixobject-chunk",
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
        out.retain(|key| key.starts_with(prefix) && !is_internal_temp_key(key));
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
        Ok(ObjectMetadata::from_parts(
            key,
            self.uri(key),
            size_bytes,
            checksum_sha256,
        ))
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
        let path = self.resolve(key)?;
        self.ensure_parent_dir(&path).await?;
        let mut source_file = tokio::fs::File::open(source).await?;
        let mut destination = match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(ObjectStoreError::AlreadyExists(key.to_string()));
            }
            Err(err) => return Err(ObjectStoreError::Io(err)),
        };
        let result = async {
            let size_bytes = tokio::io::copy(&mut source_file, &mut destination).await?;
            destination.flush().await?;
            if self.sync_writes {
                destination.sync_all().await?;
            }
            drop(destination);
            if self.sync_parent_dirs {
                sync_parent_dir(&path).await?;
            }
            Ok(ObjectMetadata::from_parts(
                key,
                self.uri(key),
                size_bytes,
                sha256_file_hex(&path).await?,
            ))
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&path).await;
        }
        result
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let path = self.resolve(key)?;
        self.ensure_parent_dir(&path).await?;
        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(ObjectStoreError::AlreadyExists(key.to_string()));
            }
            Err(err) => return Err(ObjectStoreError::Io(err)),
        };
        let result = async {
            file.write_all(&bytes).await?;
            file.flush().await?;
            if self.sync_writes {
                file.sync_all().await?;
            }
            drop(file);
            if self.sync_parent_dirs {
                sync_parent_dir(&path).await?;
            }
            Ok(ObjectMetadata::from_bytes(key, self.uri(key), &bytes))
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&path).await;
        }
        result
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
        let path = self.resolve(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(metadata) => Ok(ObjectMetadata::from_parts(
                key,
                self.uri(key),
                metadata.len(),
                sha256_file_hex(&path).await?,
            )),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(ObjectStoreError::NotFound(key.to_string()))
            }
            Err(err) => Err(ObjectStoreError::Io(err)),
        }
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

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ObjectStoreError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let path = self.resolve(key)?;
        let mut file = match tokio::fs::File::open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjectStoreError::NotFound(key.to_string()));
            }
            Err(err) => return Err(ObjectStoreError::Io(err)),
        };
        let object_len = file.metadata().await?.len();
        if offset >= object_len {
            return Ok(Bytes::new());
        }
        let bytes_to_read = length.min((object_len - offset) as usize);
        let mut buffer = vec![0u8; bytes_to_read];
        file.seek(SeekFrom::Start(offset)).await?;
        file.read_exact(&mut buffer).await?;
        Ok(Bytes::from(buffer))
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
        let tmp_path = temp_sibling_path(destination, "matrixobjectstore-download-tmp");
        let result = async {
            match tokio::fs::copy(&source, &tmp_path).await {
                Ok(size_bytes) => {
                    let checksum_sha256 = sha256_file_hex(&tmp_path).await?;
                    if self.sync_writes {
                        sync_file_path(&tmp_path).await?;
                    }
                    tokio::fs::rename(&tmp_path, destination).await?;
                    if self.sync_parent_dirs {
                        sync_parent_dir(destination).await?;
                    }
                    Ok(ObjectMetadata::from_parts(
                        key,
                        self.uri(key),
                        size_bytes,
                        checksum_sha256,
                    ))
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    Err(ObjectStoreError::NotFound(key.to_string()))
                }
                Err(err) => Err(ObjectStoreError::Io(err)),
            }
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }
        result
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
        out.retain(|key| key.starts_with(prefix) && !is_internal_temp_key(key));
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

    async fn copy_object(
        &self,
        source_key: &str,
        destination_key: &str,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let source = self.resolve(source_key)?;
        if !source.exists() {
            return Err(ObjectStoreError::NotFound(source_key.to_string()));
        }
        self.put_path_atomic(destination_key, &source).await
    }

    fn uri(&self, key: &str) -> String {
        format!("{}://{}", self.uri_scheme, key)
    }

    fn capabilities(&self) -> ObjectStoreCapabilities {
        ObjectStoreCapabilities::file(self.uri_scheme.clone())
    }

    fn topology(&self) -> ObjectStoreTopology {
        ObjectStoreTopology {
            backend: self.capabilities().backend,
            uri_scheme: self.uri_scheme.clone(),
            namespace: None,
            key_prefix: None,
            services: vec![ObjectStoreServiceDescriptor::local(
                "object",
                self.root.clone(),
            )],
        }
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

    fn validate_manifest_for_read(
        &self,
        manifest: &MatrixObjectManifest,
    ) -> Result<usize, ObjectStoreError> {
        let object_size = usize::try_from(manifest.size_bytes).map_err(|_| {
            ObjectStoreError::Io(std::io::Error::other(format!(
                "object {} is too large for this platform: {} bytes",
                manifest.key, manifest.size_bytes
            )))
        })?;
        let mut ranges = Vec::with_capacity(manifest.blocks.len());
        for block in &manifest.blocks {
            if block.length == 0 && manifest.size_bytes > 0 {
                return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                    "object {} has zero-length block {}",
                    manifest.key, block.block_id
                ))));
            }
            let end = block.offset.checked_add(block.length).ok_or_else(|| {
                ObjectStoreError::Io(std::io::Error::other(format!(
                    "object {} block {} range overflows",
                    manifest.key, block.block_id
                )))
            })?;
            if end > manifest.size_bytes {
                return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                    "object {} block {} range exceeds object size: end {}, size {}",
                    manifest.key, block.block_id, end, manifest.size_bytes
                ))));
            }
            ranges.push((block.offset, end, &block.block_id));
        }
        ranges.sort_by_key(|(offset, _, _)| *offset);
        let mut expected_offset = 0u64;
        for (offset, end, block_id) in ranges {
            if offset != expected_offset {
                return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                    "object {} block {} range is not contiguous: expected offset {}, got {}",
                    manifest.key, block_id, expected_offset, offset
                ))));
            }
            expected_offset = end;
        }
        if expected_offset != manifest.size_bytes {
            return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "object {} manifest covers {} bytes, expected {}",
                manifest.key, expected_offset, manifest.size_bytes
            ))));
        }
        Ok(object_size)
    }

    async fn write_chunks(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<Vec<MatrixObjectBlockRef>, ObjectStoreError> {
        let chunk_target_bytes = self.chunk_target_bytes();
        let expected_chunks = if bytes.is_empty() {
            1
        } else {
            bytes.len().div_ceil(chunk_target_bytes)
        };
        let object_key = key.trim_matches('/').to_string();
        let object_key_fingerprint = object_key_fingerprint(&object_key);
        let publish_block_metadata = self.publish_block_metadata_on_write();
        if expected_chunks == 1 {
            let block = self
                .write_chunk_direct(
                    MatrixObjectChunkWrite {
                        index: 0,
                        offset: 0,
                        bytes,
                    },
                    &object_key,
                    &object_key_fingerprint,
                    publish_block_metadata,
                )
                .await?
                .1;
            return Ok(vec![block]);
        }

        let mut join_set = JoinSet::new();
        let mut blocks = Vec::with_capacity(expected_chunks);
        let mut next_offset = 0usize;
        let mut next_index = 0usize;
        while next_offset < bytes.len() || !join_set.is_empty() {
            while join_set.len() < self.transfer_concurrency() && next_offset < bytes.len() {
                let end = (next_offset + chunk_target_bytes).min(bytes.len());
                let chunk = MatrixObjectChunkWrite {
                    index: next_index,
                    offset: next_offset as u64,
                    bytes: bytes.slice(next_offset..end),
                };
                next_index += 1;
                next_offset = end;
                self.spawn_chunk_write(
                    chunk,
                    &object_key,
                    &object_key_fingerprint,
                    publish_block_metadata,
                    &mut join_set,
                );
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
        let object_key = key.trim_matches('/').to_string();
        let object_key_fingerprint = object_key_fingerprint(&object_key);
        let publish_block_metadata = self.publish_block_metadata_on_write();
        let source_size = file.metadata().await?.len();
        if source_size <= self.chunk_target_bytes() as u64 {
            let bytes = Bytes::from(tokio::fs::read(path).await?);
            let checksum = sha256_hex(&bytes);
            let block = self
                .write_chunk_direct(
                    MatrixObjectChunkWrite {
                        index: 0,
                        offset: 0,
                        bytes,
                    },
                    &object_key,
                    &object_key_fingerprint,
                    publish_block_metadata,
                )
                .await?
                .1;
            return Ok((vec![block], checksum, source_size));
        }
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
            self.spawn_chunk_write(
                chunk,
                &object_key,
                &object_key_fingerprint,
                publish_block_metadata,
                &mut join_set,
            );
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
                MatrixObjectChunkWrite {
                    index: 0,
                    offset: 0,
                    bytes: Bytes::new(),
                },
                &object_key,
                &object_key_fingerprint,
                publish_block_metadata,
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
        chunk: MatrixObjectChunkWrite,
        object_key: &str,
        object_key_fingerprint: &str,
        publish_block_metadata: bool,
        join_set: &mut JoinSet<Result<(usize, MatrixObjectBlockRef), ObjectStoreError>>,
    ) {
        let chunk_service = self.chunk_service.clone();
        let block_service = self.block_service.clone();
        let object_key = object_key.to_string();
        let object_key_fingerprint = object_key_fingerprint.to_string();
        join_set.spawn(async move {
            Self::write_chunk_with_services(
                chunk,
                object_key,
                object_key_fingerprint,
                publish_block_metadata,
                chunk_service,
                block_service,
            )
            .await
        });
    }

    async fn write_chunk_direct(
        &self,
        chunk: MatrixObjectChunkWrite,
        object_key: &str,
        object_key_fingerprint: &str,
        publish_block_metadata: bool,
    ) -> Result<(usize, MatrixObjectBlockRef), ObjectStoreError> {
        Self::write_chunk_with_services(
            chunk,
            object_key.to_string(),
            object_key_fingerprint.to_string(),
            publish_block_metadata,
            self.chunk_service.clone(),
            self.block_service.clone(),
        )
        .await
    }

    async fn write_chunk_with_services(
        chunk: MatrixObjectChunkWrite,
        object_key: String,
        object_key_fingerprint: String,
        publish_block_metadata: bool,
        chunk_service: MatrixObjectStoreChunkService,
        block_service: MatrixObjectStoreBlockService,
    ) -> Result<(usize, MatrixObjectBlockRef), ObjectStoreError> {
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
        Ok((chunk.index, block_ref))
    }

    fn spawn_chunk_read(
        &self,
        block_ref: MatrixObjectBlockRef,
        join_set: &mut JoinSet<Result<(u64, Bytes), ObjectStoreError>>,
    ) {
        let chunk_service = self.chunk_service.clone();
        let block_service = self.block_service.clone();
        let verify_block_metadata = self.verify_block_metadata_on_read();
        join_set.spawn(async move {
            Self::read_chunk_with_services(
                block_ref,
                verify_block_metadata,
                chunk_service,
                block_service,
            )
            .await
        });
    }

    async fn read_chunk_direct(
        &self,
        block_ref: MatrixObjectBlockRef,
    ) -> Result<(u64, Bytes), ObjectStoreError> {
        Self::read_chunk_with_services(
            block_ref,
            self.verify_block_metadata_on_read(),
            self.chunk_service.clone(),
            self.block_service.clone(),
        )
        .await
    }

    async fn read_chunk_with_services(
        block_ref: MatrixObjectBlockRef,
        verify_block_metadata: bool,
        chunk_service: MatrixObjectStoreChunkService,
        block_service: MatrixObjectStoreBlockService,
    ) -> Result<(u64, Bytes), ObjectStoreError> {
        if verify_block_metadata {
            let published_block_ref = block_service.get_block_ref(&block_ref.block_id).await?;
            if published_block_ref != block_ref {
                return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                    "block metadata mismatch for {}",
                    block_ref.block_id
                ))));
            }
        }
        let chunk = chunk_service.get_chunk(&block_ref.chunk_key).await?;
        if chunk.len() as u64 != block_ref.length {
            return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "chunk length mismatch for {}: expected {}, got {}",
                block_ref.chunk_key,
                block_ref.length,
                chunk.len()
            ))));
        }
        let checksum = sha256_hex(&chunk);
        if checksum != block_ref.checksum_sha256 {
            return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "chunk checksum mismatch for {}",
                block_ref.chunk_key
            ))));
        }
        Ok((block_ref.offset, chunk))
    }

    async fn read_range_from_manifest(
        &self,
        key: &str,
        manifest: &MatrixObjectManifest,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ObjectStoreError> {
        self.validate_manifest_for_read(manifest)?;
        if length == 0 || offset >= manifest.size_bytes {
            return Ok(Bytes::new());
        }
        let end_offset = offset
            .saturating_add(length as u64)
            .min(manifest.size_bytes);
        let mut out = Vec::with_capacity((end_offset - offset) as usize);
        for block_ref in manifest
            .blocks
            .iter()
            .filter(|block| block.offset < end_offset && block.offset + block.length > offset)
        {
            let (chunk_offset, chunk) = self.read_chunk_direct(block_ref.clone()).await?;
            let chunk_end = chunk_offset + chunk.len() as u64;
            if chunk_end <= offset || chunk_offset >= end_offset {
                continue;
            }
            let slice_start = offset.saturating_sub(chunk_offset) as usize;
            let slice_end = (end_offset.min(chunk_end) - chunk_offset) as usize;
            out.extend_from_slice(&chunk.slice(slice_start..slice_end));
        }
        if out.len() != (end_offset - offset) as usize {
            return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "object range read for {key} returned {} bytes, expected {}",
                out.len(),
                end_offset - offset
            ))));
        }
        Ok(Bytes::from(out))
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

    async fn copy_manifest_blocks(
        &self,
        source_key: &str,
        destination_key: &str,
        source_blocks: &[MatrixObjectBlockRef],
    ) -> Result<Vec<MatrixObjectBlockRef>, ObjectStoreError> {
        let destination_key = destination_key.trim_matches('/').to_string();
        let destination_fingerprint = object_key_fingerprint(&destination_key);
        let publish_block_metadata = self.publish_block_metadata_on_write();
        let mut copied_blocks = Vec::with_capacity(source_blocks.len());
        for (index, source_block) in source_blocks.iter().enumerate() {
            let (offset, bytes) = self.read_chunk_direct(source_block.clone()).await?;
            let result = self
                .write_chunk_direct(
                    MatrixObjectChunkWrite {
                        index,
                        offset,
                        bytes,
                    },
                    &destination_key,
                    &destination_fingerprint,
                    publish_block_metadata,
                )
                .await;
            match result {
                Ok((_, block)) => copied_blocks.push(block),
                Err(err) => {
                    let _ = self.delete_block_refs(copied_blocks).await;
                    return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                        "failed to copy MatrixObject chunks from {source_key} to {destination_key}: {err}"
                    ))));
                }
            }
        }
        Ok(copied_blocks)
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

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put_atomic_unique(key, bytes).await
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
        let object_size = self.validate_manifest_for_read(&manifest)?;
        if manifest.blocks.len() == 1 {
            let (offset, chunk) = self.read_chunk_direct(manifest.blocks[0].clone()).await?;
            if offset != 0
                || chunk.len() != object_size
                || sha256_hex(&chunk) != manifest.checksum_sha256
            {
                return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                    "object checksum mismatch for {key}"
                ))));
            }
            return Ok(chunk);
        }
        let mut out = vec![0u8; object_size];
        let mut written_bytes = 0u64;
        let mut join_set = JoinSet::new();
        let mut next_to_submit = 0usize;
        while next_to_submit < manifest.blocks.len() || !join_set.is_empty() {
            while next_to_submit < manifest.blocks.len()
                && join_set.len() < self.transfer_concurrency()
            {
                let block_ref = manifest.blocks[next_to_submit].clone();
                self.spawn_chunk_read(block_ref, &mut join_set);
                next_to_submit += 1;
            }

            let (offset, chunk) = join_set
                .join_next()
                .await
                .expect("matrixobjectstore chunk read task missing")
                .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))??;
            let offset = offset as usize;
            let end = offset + chunk.len();
            if end > out.len() {
                return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                    "object chunk range mismatch for {key}: end {end}, size {}",
                    out.len()
                ))));
            }
            out[offset..end].copy_from_slice(&chunk);
            written_bytes += chunk.len() as u64;
        }
        if written_bytes != manifest.size_bytes || sha256_hex(&out) != manifest.checksum_sha256 {
            return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "object checksum mismatch for {key}"
            ))));
        }
        Ok(Bytes::from(out))
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ObjectStoreError> {
        let manifest = self.root_service.get_manifest(key).await?;
        self.read_range_from_manifest(key, &manifest, offset, length)
            .await
    }

    async fn get_to_path(
        &self,
        key: &str,
        destination: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let manifest = self.root_service.get_manifest(key).await?;
        self.validate_manifest_for_read(&manifest)?;
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp_path = temp_sibling_path(destination, "matrixobjectstore-download-tmp");
        let result = async {
            let mut file = tokio::fs::File::create(&tmp_path).await?;
            let mut written_bytes = 0u64;

            if manifest.blocks.len() == 1 {
                let (offset, chunk) = self.read_chunk_direct(manifest.blocks[0].clone()).await?;
                if offset != 0 {
                    return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                        "object chunk range mismatch for {key}: expected offset 0, got {offset}"
                    ))));
                }
                file.write_all(&chunk).await?;
                written_bytes += chunk.len() as u64;
            } else {
                file.set_len(manifest.size_bytes).await?;
                let mut join_set = JoinSet::new();
                let mut next_to_submit = 0usize;
                while next_to_submit < manifest.blocks.len() || !join_set.is_empty() {
                    while next_to_submit < manifest.blocks.len()
                        && join_set.len() < self.transfer_concurrency()
                    {
                        let block_ref = manifest.blocks[next_to_submit].clone();
                        self.spawn_chunk_read(block_ref, &mut join_set);
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
            }
            file.flush().await?;
            if self.config.sync_writes {
                file.sync_all().await?;
            }
            drop(file);

            if written_bytes != manifest.size_bytes {
                return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                    "object size mismatch for {key}: expected {}, wrote {}",
                    manifest.size_bytes, written_bytes
                ))));
            }
            let checksum_sha256 = sha256_file_hex(&tmp_path).await?;
            if checksum_sha256 != manifest.checksum_sha256 {
                return Err(ObjectStoreError::Io(std::io::Error::other(format!(
                    "object checksum mismatch for {key}"
                ))));
            }
            tokio::fs::rename(&tmp_path, destination).await?;
            if self.config.sync_parent_dirs {
                sync_parent_dir(destination).await?;
            }
            Ok(ObjectMetadata::from_parts(
                manifest.key,
                manifest.uri,
                manifest.size_bytes,
                checksum_sha256,
            ))
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }
        result
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.root_service.list_manifest_keys(prefix).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        continuation_token: Option<&str>,
        max_keys: usize,
    ) -> Result<ObjectListPage, ObjectStoreError> {
        self.root_service
            .list_manifest_keys_page(prefix, continuation_token, max_keys)
            .await
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

    async fn copy_object(
        &self,
        source_key: &str,
        destination_key: &str,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let source_manifest = self.root_service.get_manifest(source_key).await?;
        self.validate_manifest_for_read(&source_manifest)?;
        let previous_manifest = self.root_service.get_manifest(destination_key).await.ok();
        let blocks = self
            .copy_manifest_blocks(source_key, destination_key, &source_manifest.blocks)
            .await?;
        let live_chunk_keys: HashSet<String> =
            blocks.iter().map(|block| block.chunk_key.clone()).collect();
        let live_block_ids: HashSet<String> =
            blocks.iter().map(|block| block.block_id.clone()).collect();
        let manifest = MatrixObjectManifest {
            key: destination_key.to_string(),
            uri: self.uri(destination_key),
            size_bytes: source_manifest.size_bytes,
            checksum_sha256: source_manifest.checksum_sha256,
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
        Ok(ObjectMetadata::from_parts(
            manifest.key,
            manifest.uri,
            manifest.size_bytes,
            manifest.checksum_sha256,
        ))
    }

    fn uri(&self, key: &str) -> String {
        self.root_service.uri(key)
    }

    fn capabilities(&self) -> ObjectStoreCapabilities {
        ObjectStoreCapabilities::matrixobject(self.config.uri_scheme.clone(), true)
    }

    fn topology(&self) -> ObjectStoreTopology {
        self.service_topology()
            .as_generic(self.config.uri_scheme.clone())
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectStoreError> {
        let manifest = self.root_service.get_manifest(key).await?;
        self.validate_manifest_for_read(&manifest)?;
        Ok(ObjectMetadata::from_parts(
            manifest.key,
            manifest.uri,
            manifest.size_bytes,
            manifest.checksum_sha256,
        ))
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
        match self.root_service.get_manifest(key).await {
            Ok(_) => return Err(ObjectStoreError::AlreadyExists(key.to_string())),
            Err(ObjectStoreError::NotFound(_)) => {}
            Err(err) => return Err(err),
        }
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
        Ok(ObjectMetadata::from_parts(
            manifest.key,
            manifest.uri,
            manifest.size_bytes,
            manifest.checksum_sha256,
        ))
    }

    async fn put_atomic_inner(
        &self,
        key: &str,
        bytes: Bytes,
        cleanup_previous: bool,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if !cleanup_previous {
            match self.root_service.get_manifest(key).await {
                Ok(_) => return Err(ObjectStoreError::AlreadyExists(key.to_string())),
                Err(ObjectStoreError::NotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
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
        Ok(ObjectMetadata::from_parts(
            manifest.key,
            manifest.uri,
            manifest.size_bytes,
            manifest.checksum_sha256,
        ))
    }
}

#[async_trait]
impl ObjectStore for RemoteObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.put_atomic(key, bytes).await.map(|_| ())
    }

    async fn put_unique(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.put_if_absent(key, bytes).await.map(|_| ())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let response = self
            .http_request(
                self.plan_put(key)?,
                vec![("If-None-Match".to_string(), "*".to_string())],
                bytes.clone(),
            )
            .await?;
        map_http_status(response.status, key, || Ok(()))?;
        Ok(ObjectMetadata::from_bytes(key, self.uri(key), &bytes))
    }

    async fn put_path_unique(
        &self,
        key: &str,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let bytes = Bytes::from(tokio::fs::read(path).await?);
        self.put_if_absent(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let response = self
            .http_request(self.plan_get(key)?, Vec::new(), Bytes::new())
            .await?;
        map_http_status(response.status, key, || Ok(()))?;
        Ok(response.body)
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ObjectStoreError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = offset.saturating_add(length as u64).saturating_sub(1);
        let response = self
            .http_request(
                self.plan_get(key)?,
                vec![("Range".to_string(), format!("bytes={offset}-{end}"))],
                Bytes::new(),
            )
            .await?;
        map_http_status(response.status, key, || Ok(()))?;
        Ok(response.body)
    }

    async fn get_to_path(
        &self,
        key: &str,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let bytes = self.get(key).await?;
        write_object_file(path, &bytes).await?;
        Ok(ObjectMetadata::from_bytes(key, self.uri(key), &bytes))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let mut out = Vec::new();
        let mut continuation_token = None;
        loop {
            let page = self
                .list_page(prefix, continuation_token.as_deref(), 1024)
                .await?;
            if page.keys.is_empty() {
                break;
            }
            continuation_token = page.next_continuation_token.clone();
            out.extend(page.keys);
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(out)
    }

    async fn list_page(
        &self,
        prefix: &str,
        continuation_token: Option<&str>,
        max_keys: usize,
    ) -> Result<ObjectListPage, ObjectStoreError> {
        let response = self
            .http_request(
                self.plan_list(prefix, continuation_token, max_keys)?,
                Vec::new(),
                Bytes::new(),
            )
            .await?;
        map_http_status(response.status, prefix, || Ok(()))?;
        let body = String::from_utf8_lossy(&response.body);
        let keys = parse_s3_list_keys(&body)
            .into_iter()
            .filter_map(|key| self.public_key_from_remote(&key))
            .collect::<Vec<_>>();
        Ok(ObjectListPage {
            keys,
            next_continuation_token: parse_s3_next_continuation_token(&body),
        })
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let response = self
            .http_request(self.plan_delete(key)?, Vec::new(), Bytes::new())
            .await?;
        match response.status {
            200..=299 | 404 => Ok(()),
            status => Err(ObjectStoreError::Io(std::io::Error::other(format!(
                "remote object HTTP status {status} for {key}"
            )))),
        }
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize, ObjectStoreError> {
        let mut deleted = 0usize;
        for key in keys {
            self.delete(key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    async fn copy_object(
        &self,
        source_key: &str,
        destination_key: &str,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let response = self
            .http_request(
                self.plan_copy(source_key, destination_key)?,
                Vec::new(),
                Bytes::new(),
            )
            .await?;
        map_http_status(response.status, destination_key, || Ok(()))?;
        self.http_metadata_from_get(destination_key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectStoreError> {
        let response = self
            .http_request(self.plan_head(key)?, Vec::new(), Bytes::new())
            .await?;
        map_http_status(response.status, key, || Ok(()))?;
        let size_bytes = http_header(&response.headers, "content-length")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let mut metadata = ObjectMetadata::from_parts(key, self.uri(key), size_bytes, "");
        metadata.etag = http_header(&response.headers, "etag").map(ToString::to_string);
        metadata.version_id =
            http_header(&response.headers, "x-amz-version-id").map(ToString::to_string);
        Ok(metadata)
    }

    async fn put_atomic(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let response = self
            .http_request(self.plan_put(key)?, Vec::new(), bytes.clone())
            .await?;
        map_http_status(response.status, key, || Ok(()))?;
        Ok(ObjectMetadata::from_bytes(key, self.uri(key), &bytes))
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<usize, ObjectStoreError> {
        let keys = self.list(prefix).await?;
        self.delete_objects(&keys).await
    }

    fn uri(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        let scheme = self.backend.uri_scheme();
        if let Some(namespace) = &self.namespace {
            if let Some(prefix) = &self.key_prefix {
                if key.is_empty() {
                    format!("{scheme}://{namespace}/{prefix}")
                } else {
                    format!("{scheme}://{namespace}/{prefix}/{key}")
                }
            } else if key.is_empty() {
                format!("{scheme}://{namespace}")
            } else {
                format!("{scheme}://{namespace}/{key}")
            }
        } else {
            let base = self.uri.trim_end_matches('/');
            if base.ends_with("://") {
                format!("{base}{key}")
            } else if key.is_empty() {
                base.to_string()
            } else {
                format!("{base}/{key}")
            }
        }
    }

    fn capabilities(&self) -> ObjectStoreCapabilities {
        let mut capabilities = ObjectStoreCapabilities::remote_expected(self.backend);
        if self.supports_unsigned_http() {
            capabilities.runtime_linked = true;
            capabilities.operations_fail_closed = false;
        }
        capabilities
    }

    fn topology(&self) -> ObjectStoreTopology {
        ObjectStoreTopology {
            backend: self.backend.canonical_name().to_string(),
            uri_scheme: self.backend.uri_scheme().to_string(),
            namespace: self.namespace.clone(),
            key_prefix: self.key_prefix.clone(),
            services: vec![ObjectStoreServiceDescriptor::endpoint(
                "object",
                self.endpoint.clone().or_else(|| Some(self.uri.clone())),
            )],
        }
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

async fn sync_file_path(path: &Path) -> Result<(), ObjectStoreError> {
    let file = tokio::fs::File::open(path).await?;
    file.sync_all().await?;
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

fn temp_sibling_path(path: &Path, label: &str) -> PathBuf {
    path.with_extension(format!("{label}-{}", Uuid::new_v4().simple()))
}

fn is_internal_temp_key(key: &str) -> bool {
    key.contains("matrixobjectstore-tmp") || key.contains("matrixobjectstore-download-tmp")
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
            Self::Remote(store) => store.put(key, bytes).await,
        }
    }

    async fn put_unique(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.put_unique(key, bytes).await,
            Self::MatrixObjectStore(store) => store.put_unique(key, bytes).await,
            Self::Remote(store) => store.put_unique(key, bytes).await,
        }
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => {
                store.put_if_absent(key, bytes).await
            }
            Self::MatrixObjectStore(store) => store.put_if_absent(key, bytes).await,
            Self::Remote(store) => store.put_if_absent(key, bytes).await,
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
            Self::Remote(store) => store.put_path_unique(key, path).await,
        }
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.get(key).await,
            Self::MatrixObjectStore(store) => store.get(key).await,
            Self::Remote(store) => store.get(key).await,
        }
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => {
                store.get_range(key, offset, length).await
            }
            Self::MatrixObjectStore(store) => store.get_range(key, offset, length).await,
            Self::Remote(store) => store.get_range(key, offset, length).await,
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
            Self::Remote(store) => store.get_to_path(key, path).await,
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.list(prefix).await,
            Self::MatrixObjectStore(store) => store.list(prefix).await,
            Self::Remote(store) => store.list(prefix).await,
        }
    }

    async fn list_page(
        &self,
        prefix: &str,
        continuation_token: Option<&str>,
        max_keys: usize,
    ) -> Result<ObjectListPage, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => {
                store.list_page(prefix, continuation_token, max_keys).await
            }
            Self::MatrixObjectStore(store) => {
                store.list_page(prefix, continuation_token, max_keys).await
            }
            Self::Remote(store) => store.list_page(prefix, continuation_token, max_keys).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.delete(key).await,
            Self::MatrixObjectStore(store) => store.delete(key).await,
            Self::Remote(store) => store.delete(key).await,
        }
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.delete_objects(keys).await,
            Self::MatrixObjectStore(store) => store.delete_objects(keys).await,
            Self::Remote(store) => store.delete_objects(keys).await,
        }
    }

    async fn copy_object(
        &self,
        source_key: &str,
        destination_key: &str,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => {
                store.copy_object(source_key, destination_key).await
            }
            Self::MatrixObjectStore(store) => store.copy_object(source_key, destination_key).await,
            Self::Remote(store) => store.copy_object(source_key, destination_key).await,
        }
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<usize, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.delete_prefix(prefix).await,
            Self::MatrixObjectStore(store) => store.delete_prefix(prefix).await,
            Self::Remote(store) => store.delete_prefix(prefix).await,
        }
    }

    fn uri(&self, key: &str) -> String {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.uri(key),
            Self::MatrixObjectStore(store) => store.uri(key),
            Self::Remote(store) => store.uri(key),
        }
    }

    fn capabilities(&self) -> ObjectStoreCapabilities {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.capabilities(),
            Self::MatrixObjectStore(store) => store.capabilities(),
            Self::Remote(store) => store.capabilities(),
        }
    }

    fn topology(&self) -> ObjectStoreTopology {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.topology(),
            Self::MatrixObjectStore(store) => store.topology(),
            Self::Remote(store) => store.topology(),
        }
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, ObjectStoreError> {
        match self {
            Self::LocalFile(store) | Self::SharedFile(store) => store.head(key).await,
            Self::MatrixObjectStore(store) => store.head(key).await,
            Self::Remote(store) => store.head(key).await,
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
            Self::Remote(store) => store.put_atomic(key, bytes).await,
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn matrix_object_store_put_atomic_returns_checksum_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = MatrixObjectStore::with_uri_scheme(dir.path(), "matrixobject");

        let metadata = store
            .put_atomic(
                "tenant/a/blob-1",
                Bytes::from_static(b"hello matrix object store"),
            )
            .await
            .unwrap();

        assert_eq!(metadata.key, "tenant/a/blob-1");
        assert_eq!(metadata.uri, "matrixobject://tenant/a/blob-1");
        assert_eq!(metadata.size_bytes, 25);
        assert_eq!(
            metadata.checksum_sha256,
            "32bf29e5bb7440b15303a464d7e8e0c4e2a94c026e0d9820bdba0a6a8a0dc5a9"
        );
        assert_eq!(
            metadata.etag.as_deref(),
            Some("sha256:32bf29e5bb7440b15303a464d7e8e0c4e2a94c026e0d9820bdba0a6a8a0dc5a9")
        );
        assert_eq!(metadata.version_id, None);
        assert_eq!(
            store.get("tenant/a/blob-1").await.unwrap(),
            Bytes::from_static(b"hello matrix object store")
        );
        assert_eq!(store.head("tenant/a/blob-1").await.unwrap(), metadata);
    }

    #[tokio::test]
    async fn object_store_capabilities_and_topology_are_generic() {
        let dir = tempfile::tempdir().unwrap();
        let shared = SharedObjectStore::from_backend_root(
            SharedObjectStoreBackend::MatrixObjectStore,
            dir.path(),
        )
        .unwrap();

        let capabilities = shared.capabilities();
        assert_eq!(capabilities.backend, "matrixobject");
        assert_eq!(capabilities.uri_scheme, "matrixobject");
        assert!(capabilities.runtime_linked);
        assert!(!capabilities.operations_fail_closed);
        assert!(capabilities.atomic_put);
        assert!(capabilities.unique_put);
        assert!(capabilities.conditional_create);
        assert!(capabilities.copy_object);
        assert!(capabilities.delete_prefix);
        assert!(capabilities.paginated_list);
        assert!(capabilities.bulk_delete);
        assert!(capabilities.byte_range_read);
        assert!(capabilities.checksum_sha256);
        assert!(capabilities.object_etag);
        assert!(!capabilities.object_version_id);
        assert!(capabilities.split_services);

        let topology = shared.topology();
        assert_eq!(topology.backend, "matrixobject");
        assert_eq!(topology.uri_scheme, "matrixobject");
        assert_eq!(topology.namespace, None);
        assert_eq!(topology.key_prefix, None);
        assert_eq!(
            topology
                .services
                .iter()
                .map(|service| service.role.as_str())
                .collect::<Vec<_>>(),
            vec!["root", "block", "chunk"]
        );
        assert!(topology
            .services
            .iter()
            .all(|service| service.local_root.is_some()));
    }

    #[tokio::test]
    async fn generic_object_store_range_reads_work_for_file_and_matrixobject() {
        let dir = tempfile::tempdir().unwrap();
        let file_store = FileObjectStore::new(dir.path().join("file"));
        file_store
            .put_atomic("objects/range.txt", Bytes::from_static(b"0123456789abcdef"))
            .await
            .unwrap();
        assert_eq!(
            file_store
                .get_range("objects/range.txt", 4, 6)
                .await
                .unwrap(),
            Bytes::from_static(b"456789")
        );
        assert_eq!(
            file_store
                .get_range("objects/range.txt", 99, 6)
                .await
                .unwrap(),
            Bytes::new()
        );

        let matrix_store = MatrixObjectStore::from_config(
            MatrixObjectStoreConfig::local_compat(dir.path().join("matrix"))
                .with_chunk_target_bytes(4),
        );
        matrix_store
            .put_atomic("objects/range.txt", Bytes::from_static(b"0123456789abcdef"))
            .await
            .unwrap();
        assert_eq!(
            matrix_store
                .get_range("objects/range.txt", 3, 10)
                .await
                .unwrap(),
            Bytes::from_static(b"3456789abc")
        );
        assert_eq!(
            matrix_store
                .get_range("objects/range.txt", 15, 10)
                .await
                .unwrap(),
            Bytes::from_static(b"f")
        );
    }

    #[tokio::test]
    async fn generic_object_store_copy_and_delete_prefix_work_for_file_and_matrixobject() {
        let dir = tempfile::tempdir().unwrap();
        let file_store = SharedObjectStore::from_backend_root(
            SharedObjectStoreBackend::LocalFile,
            dir.path().join("file"),
        )
        .unwrap();
        file_store
            .put_atomic("snapshots/a", Bytes::from_static(b"file payload"))
            .await
            .unwrap();
        let copied = file_store
            .copy_object("snapshots/a", "snapshots/copy/a")
            .await
            .unwrap();
        assert_eq!(copied.key, "snapshots/copy/a");
        assert_eq!(
            file_store.get("snapshots/copy/a").await.unwrap(),
            Bytes::from_static(b"file payload")
        );
        assert_eq!(file_store.delete_prefix("snapshots/").await.unwrap(), 2);
        assert!(matches!(
            file_store.get("snapshots/a").await,
            Err(ObjectStoreError::NotFound(_))
        ));

        let matrix_store = MatrixObjectStore::from_config(
            MatrixObjectStoreConfig::local_compat(dir.path().join("matrix"))
                .with_chunk_target_bytes(4),
        );
        matrix_store
            .put_atomic("objects/src", Bytes::from_static(b"0123456789abcdef"))
            .await
            .unwrap();
        let copied = matrix_store
            .copy_object("objects/src", "objects/copied")
            .await
            .unwrap();
        assert_eq!(copied.key, "objects/copied");
        assert_eq!(copied.size_bytes, 16);
        assert_eq!(
            matrix_store.get("objects/copied").await.unwrap(),
            Bytes::from_static(b"0123456789abcdef")
        );
        matrix_store.delete("objects/src").await.unwrap();
        assert_eq!(
            matrix_store.get("objects/copied").await.unwrap(),
            Bytes::from_static(b"0123456789abcdef")
        );
        assert_eq!(matrix_store.delete_prefix("objects/").await.unwrap(), 1);
        assert!(matrix_store.list("objects/").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn generic_object_store_put_if_absent_rejects_existing_objects() {
        async fn assert_put_if_absent<O: ObjectStore>(store: &O) {
            let first = store
                .put_if_absent("objects/only-once", Bytes::from_static(b"first"))
                .await
                .unwrap();
            assert_eq!(first.key, "objects/only-once");
            assert!(matches!(
                store
                    .put_if_absent("objects/only-once", Bytes::from_static(b"second"))
                    .await,
                Err(ObjectStoreError::AlreadyExists(key)) if key == "objects/only-once"
            ));
            assert_eq!(
                store.get("objects/only-once").await.unwrap(),
                Bytes::from_static(b"first")
            );
        }

        let dir = tempfile::tempdir().unwrap();
        let file_store = SharedObjectStore::from_backend_root(
            SharedObjectStoreBackend::LocalFile,
            dir.path().join("file"),
        )
        .unwrap();
        assert_put_if_absent(&file_store).await;

        let matrix_store = MatrixObjectStore::from_config(
            MatrixObjectStoreConfig::local_compat(dir.path().join("matrix"))
                .with_chunk_target_bytes(4),
        );
        assert_put_if_absent(&matrix_store).await;
    }

    #[tokio::test]
    async fn generic_object_store_delete_objects_works_for_file_and_matrixobject() {
        async fn assert_delete_objects<O: ObjectStore>(store: &O) {
            for key in ["objects/a", "objects/b", "objects/c"] {
                store
                    .put(key, Bytes::from_static(b"payload"))
                    .await
                    .unwrap();
            }
            let deleted = store
                .delete_objects(&["objects/a".to_string(), "objects/c".to_string()])
                .await
                .unwrap();
            assert_eq!(deleted, 2);
            assert_eq!(store.list("objects/").await.unwrap(), vec!["objects/b"]);
        }

        let dir = tempfile::tempdir().unwrap();
        let file_store = SharedObjectStore::from_backend_root(
            SharedObjectStoreBackend::LocalFile,
            dir.path().join("file"),
        )
        .unwrap();
        assert_delete_objects(&file_store).await;

        let matrix_store = MatrixObjectStore::from_config(
            MatrixObjectStoreConfig::local_compat(dir.path().join("matrix"))
                .with_chunk_target_bytes(4),
        );
        assert_delete_objects(&matrix_store).await;
    }

    #[tokio::test]
    async fn generic_object_store_paginated_list_works_for_file_and_matrixobject() {
        async fn assert_pages<O: ObjectStore>(store: &O) {
            for key in [
                "objects/a",
                "objects/b",
                "objects/c",
                "objects/d",
                "other/ignored",
            ] {
                store
                    .put(key, Bytes::from_static(b"payload"))
                    .await
                    .unwrap();
            }

            let first = store.list_page("objects/", None, 2).await.unwrap();
            assert_eq!(first.keys, vec!["objects/a", "objects/b"]);
            assert_eq!(first.next_continuation_token.as_deref(), Some("objects/b"));

            let second = store
                .list_page("objects/", first.next_continuation_token.as_deref(), 2)
                .await
                .unwrap();
            assert_eq!(second.keys, vec!["objects/c", "objects/d"]);
            assert!(second.next_continuation_token.is_none());

            let empty = store
                .list_page("objects/", Some("objects/d"), 2)
                .await
                .unwrap();
            assert!(empty.keys.is_empty());
            assert!(empty.next_continuation_token.is_none());
        }

        let dir = tempfile::tempdir().unwrap();
        let file_store = SharedObjectStore::from_backend_root(
            SharedObjectStoreBackend::LocalFile,
            dir.path().join("file"),
        )
        .unwrap();
        assert_pages(&file_store).await;

        let matrix_store = MatrixObjectStore::from_config(
            MatrixObjectStoreConfig::local_compat(dir.path().join("matrix"))
                .with_chunk_target_bytes(4),
        );
        assert_pages(&matrix_store).await;
        let first = matrix_store.list_page("objects/", None, 1).await.unwrap();
        assert_eq!(first.keys, vec!["objects/a"]);
        assert_eq!(first.next_continuation_token.as_deref(), Some("objects/a"));
        assert!(!first
            .next_continuation_token
            .as_deref()
            .unwrap()
            .contains(".manifest.json"));
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
    async fn matrix_object_store_failed_download_preserves_destination() {
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
        let manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();
        store
            .chunk_service
            .chunk_store
            .put_atomic(
                &manifest.blocks[0].chunk_key,
                Bytes::from_static(b"corrupt-chunk"),
            )
            .await
            .unwrap();

        let destination = dir.path().join("restore/large-object.bin");
        tokio::fs::create_dir_all(destination.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&destination, b"previous-good-restore")
            .await
            .unwrap();
        assert!(store.get("large/object").await.is_err());
        assert!(store
            .get_to_path("large/object", &destination)
            .await
            .is_err());
        assert_eq!(
            tokio::fs::read(&destination).await.unwrap(),
            b"previous-good-restore"
        );

        let mut manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();
        manifest.blocks[0].offset = 1;
        store.root_service.put_manifest(&manifest).await.unwrap();

        assert!(store.head("large/object").await.is_err());
        assert!(store.get("large/object").await.is_err());
        assert!(store
            .get_to_path("large/object", &destination)
            .await
            .is_err());

        let mut manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();
        manifest.blocks[1].offset = 0;
        store.root_service.put_manifest(&manifest).await.unwrap();

        assert!(store.head("large/object").await.is_err());
        assert!(store.get("large/object").await.is_err());
    }

    #[tokio::test]
    async fn matrix_object_store_rejects_invalid_manifest_ranges_before_read() {
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
        let mut manifest = store
            .root_service
            .get_manifest("large/object")
            .await
            .unwrap();
        manifest.blocks[0].offset = manifest.size_bytes + 1;
        store.root_service.put_manifest(&manifest).await.unwrap();

        assert!(store.head("large/object").await.is_err());
        assert!(store.get("large/object").await.is_err());
        let destination = dir.path().join("restore/large-object.bin");
        tokio::fs::create_dir_all(destination.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&destination, b"previous-good-restore")
            .await
            .unwrap();
        assert!(store
            .get_to_path("large/object", &destination)
            .await
            .is_err());
        assert_eq!(
            tokio::fs::read(&destination).await.unwrap(),
            b"previous-good-restore"
        );
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

        store
            .block_service
            .put_block_ref(&manifest.blocks[0])
            .await
            .unwrap();
        let mut corrupted_block_ref = manifest.blocks[1].clone();
        corrupted_block_ref.length += 1;
        store
            .block_service
            .put_block_ref(&corrupted_block_ref)
            .await
            .unwrap();

        assert!(matches!(
            store.get("large/object").await,
            Err(ObjectStoreError::Io(_))
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
        let config = MatrixObjectStoreConfig::external("matrixobject://cluster-a", dir.path())
            .with_uri_scheme("matrixobject")
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
            Some("matrixobject://cluster-a")
        );
        let metadata = store
            .put_atomic("snapshots/a", Bytes::from_static(b"payload"))
            .await
            .unwrap();
        assert_eq!(metadata.uri, "matrixobject://snapshots/a");
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
        let destination = dir.path().join("restore/local-object");
        let metadata = store
            .get_to_path("fast/local-object", &destination)
            .await
            .unwrap();
        assert_eq!(metadata.size_bytes, 25);
        assert_eq!(
            tokio::fs::read(&destination).await.unwrap(),
            b"fast shared-store payload"
        );
    }

    #[tokio::test]
    async fn matrix_object_store_supports_split_service_endpoints() {
        let dir = tempfile::tempdir().unwrap();
        let config = MatrixObjectStoreConfig::external("matrixobject://cluster-a", dir.path())
            .with_service_endpoints(MatrixObjectStoreServiceEndpoints::split(
                "matrixobject-root://root-1",
                "matrixobject-block://block-1",
                "matrixobject-chunk://chunk-1",
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
            Some("matrixobject-root://root-1")
        );
        assert_eq!(
            topology.block_service.endpoint.as_deref(),
            Some("matrixobject-block://block-1")
        );
        assert_eq!(
            topology.chunk_service.endpoint.as_deref(),
            Some("matrixobject-chunk://chunk-1")
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
        assert_eq!(store.head("objects/payload.bin").await.unwrap(), metadata);
    }

    #[tokio::test]
    async fn file_object_store_failed_download_preserves_destination() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileObjectStore::new(dir.path());
        let destination = dir.path().join("restore/payload.bin");
        tokio::fs::create_dir_all(destination.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&destination, b"previous-good-restore")
            .await
            .unwrap();

        assert!(matches!(
            store.get_to_path("missing/payload.bin", &destination).await,
            Err(ObjectStoreError::NotFound(_))
        ));
        assert_eq!(
            tokio::fs::read(&destination).await.unwrap(),
            b"previous-good-restore"
        );
    }

    #[tokio::test]
    async fn file_object_store_list_hides_internal_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileObjectStore::new(dir.path());
        store
            .put_atomic("objects/live.bin", Bytes::from_static(b"live"))
            .await
            .unwrap();
        tokio::fs::write(
            dir.path()
                .join("objects/live.matrixobjectstore-tmp-leftover"),
            b"stale-upload-temp",
        )
        .await
        .unwrap();
        tokio::fs::write(
            dir.path()
                .join("objects/live.matrixobjectstore-download-tmp-leftover"),
            b"stale-download-temp",
        )
        .await
        .unwrap();

        assert_eq!(
            store.list("objects/").await.unwrap(),
            vec!["objects/live.bin".to_string()]
        );
    }

    #[tokio::test]
    async fn shared_object_store_backend_contract_normalizes_aliases() {
        assert_eq!(
            SharedObjectStoreBackend::from_uri("matrixobject://bucket/a"),
            SharedObjectStoreBackend::MatrixObjectStore
        );
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
        assert_eq!(
            SharedObjectStoreBackend::parse("matrix_object_store").canonical_name(),
            "matrixobject"
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
        assert_eq!(config.canonical_backend_name(), "matrixobject");
        assert_eq!(config.uri_scheme(), "matrixobject");
        let store = SharedObjectStore::from_config(config).unwrap();

        let metadata = store
            .put_atomic("shared/key", Bytes::from_static(b"shared payload"))
            .await
            .unwrap();

        assert_eq!(metadata.uri, "matrixobject://shared/key");
        let expected_etag = format!("sha256:{}", metadata.checksum_sha256);
        assert_eq!(metadata.etag.as_deref(), Some(expected_etag.as_str()));
        assert_eq!(metadata.version_id, None);
        assert_eq!(store.head("shared/key").await.unwrap().etag, metadata.etag);
        assert_eq!(
            store.get("shared/key").await.unwrap(),
            Bytes::from_static(b"shared payload")
        );
    }

    #[tokio::test]
    async fn shared_object_store_remote_backends_report_contract_and_fail_closed_until_linked() {
        let dir = tempfile::tempdir().unwrap();
        let store = SharedObjectStore::from_config(
            SharedObjectStoreConfig::from_uri("s3://bucket/prefix", dir.path())
                .with_endpoint("https://s3.example.invalid"),
        )
        .unwrap();

        let capabilities = store.capabilities();
        assert_eq!(capabilities.backend, "s3");
        assert_eq!(capabilities.uri_scheme, "s3");
        assert!(!capabilities.runtime_linked);
        assert!(capabilities.operations_fail_closed);
        assert!(capabilities.atomic_put);
        assert!(capabilities.conditional_create);
        assert!(capabilities.copy_object);
        assert!(capabilities.delete_prefix);
        assert!(capabilities.paginated_list);
        assert!(capabilities.bulk_delete);
        assert!(capabilities.byte_range_read);
        assert!(capabilities.object_etag);
        assert!(capabilities.object_version_id);

        let topology = store.topology();
        assert_eq!(topology.backend, "s3");
        assert_eq!(topology.uri_scheme, "s3");
        assert_eq!(topology.namespace.as_deref(), Some("bucket"));
        assert_eq!(topology.key_prefix.as_deref(), Some("prefix"));
        assert_eq!(topology.services.len(), 1);
        assert_eq!(topology.services[0].role, "object");
        assert_eq!(
            topology.services[0].endpoint.as_deref(),
            Some("https://s3.example.invalid")
        );
        assert_eq!(store.uri("object"), "s3://bucket/prefix/object");
        assert_eq!(
            store.uri("/nested/object"),
            "s3://bucket/prefix/nested/object"
        );

        assert!(matches!(
            store.get("prefix/object").await,
            Err(ObjectStoreError::UnsupportedBackend { backend, .. }) if backend == "s3"
        ));
    }

    #[tokio::test]
    async fn remote_object_store_unsigned_http_s3_path_supports_basic_object_ops() {
        let server = TestS3Server::start().await;
        let store = SharedObjectStore::from_config(
            SharedObjectStoreConfig::from_uri("s3://bucket/base", "/tmp/unused")
                .with_endpoint(server.endpoint.clone()),
        )
        .unwrap();

        let metadata = store
            .put_atomic("objects/a.bin", Bytes::from_static(b"hello remote object"))
            .await
            .unwrap();
        assert_eq!(metadata.uri, "s3://bucket/base/objects/a.bin");
        assert_eq!(
            store.get("objects/a.bin").await.unwrap(),
            Bytes::from_static(b"hello remote object")
        );
        assert!(matches!(
            store
                .put_if_absent("objects/a.bin", Bytes::from_static(b"duplicate"))
                .await,
            Err(ObjectStoreError::AlreadyExists(_))
        ));
        assert_eq!(
            store
                .put_if_absent("objects/unique.bin", Bytes::from_static(b"unique"))
                .await
                .unwrap()
                .size_bytes,
            6
        );
        assert_eq!(
            store.get_range("objects/a.bin", 6, 6).await.unwrap(),
            Bytes::from_static(b"remote")
        );
        assert_eq!(store.head("objects/a.bin").await.unwrap().size_bytes, 19);

        store
            .copy_object("objects/a.bin", "objects/b.bin")
            .await
            .unwrap();
        assert_eq!(
            store.get("objects/b.bin").await.unwrap(),
            Bytes::from_static(b"hello remote object")
        );

        let mut keys = store.list("objects/").await.unwrap();
        keys.sort();
        assert_eq!(
            keys,
            vec!["objects/a.bin", "objects/b.bin", "objects/unique.bin"]
        );

        store.delete("objects/a.bin").await.unwrap();
        assert!(matches!(
            store.get("objects/a.bin").await,
            Err(ObjectStoreError::NotFound(_))
        ));
        drop(server);
    }

    #[test]
    fn remote_object_store_plans_s3_compatible_requests_generically() {
        let store = SharedObjectStore::from_config(
            SharedObjectStoreConfig::from_uri("s3://bucket/base/prefix", "/tmp/unused")
                .with_endpoint("http://127.0.0.1:19000/"),
        )
        .unwrap();
        let SharedObjectStore::Remote(remote) = store else {
            panic!("expected remote object store");
        };

        let put = remote.plan_put("snapshots/a object.bin").unwrap();
        assert_eq!(put.backend, "s3");
        assert_eq!(put.method, "PUT");
        assert_eq!(
            put.object_key.as_deref(),
            Some("base/prefix/snapshots/a object.bin")
        );
        assert_eq!(
            put.url,
            "http://127.0.0.1:19000/bucket/base/prefix/snapshots/a%20object.bin"
        );

        let copy = remote
            .plan_copy("snapshots/a object.bin", "snapshots/copied.bin")
            .unwrap();
        assert_eq!(copy.method, "PUT");
        assert_eq!(
            copy.copy_source.as_deref(),
            Some("/bucket/base/prefix/snapshots/a%20object.bin")
        );
        assert_eq!(
            copy.url,
            "http://127.0.0.1:19000/bucket/base/prefix/snapshots/copied.bin"
        );

        let list = remote
            .plan_list("snapshots/", Some("base/prefix/snapshots/a object.bin"), 17)
            .unwrap();
        assert_eq!(list.method, "GET");
        assert_eq!(
            list.url,
            "http://127.0.0.1:19000/bucket?list-type=2&prefix=base%2Fprefix%2Fsnapshots%2F&max-keys=17&continuation-token=base%2Fprefix%2Fsnapshots%2Fa%20object.bin"
        );
    }

    #[test]
    fn remote_object_store_rejects_unsafe_planned_keys() {
        let store = SharedObjectStore::from_config(
            SharedObjectStoreConfig::from_uri("ceph+s3://bucket/prefix", "/tmp/unused")
                .with_endpoint("http://127.0.0.1:19000"),
        )
        .unwrap();
        let SharedObjectStore::Remote(remote) = store else {
            panic!("expected remote object store");
        };

        assert!(matches!(
            remote.plan_get("../escape"),
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            remote.plan_delete("nested\\escape"),
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            remote.plan_list("../escape", None, 10),
            Err(ObjectStoreError::InvalidKey(_))
        ));
    }

    #[test]
    fn shared_object_store_remote_uri_parsing_is_generic() {
        let dir = tempfile::tempdir().unwrap();
        for (uri, backend, namespace, key_prefix, object_uri) in [
            (
                "s3://bucket-a/snapshots/cluster-a/",
                "s3",
                Some("bucket-a"),
                Some("snapshots/cluster-a"),
                "s3://bucket-a/snapshots/cluster-a/manifest.json",
            ),
            (
                "ceph+s3://bucket-b/prefix",
                "ceph_s3",
                Some("bucket-b"),
                Some("prefix"),
                "ceph+s3://bucket-b/prefix/manifest.json",
            ),
            (
                "rados://pool-c",
                "ceph_rados",
                Some("pool-c"),
                None,
                "rados://pool-c/manifest.json",
            ),
        ] {
            let store =
                SharedObjectStore::from_config(SharedObjectStoreConfig::from_uri(uri, dir.path()))
                    .unwrap();
            let topology = store.topology();
            assert_eq!(topology.backend, backend);
            assert_eq!(topology.namespace.as_deref(), namespace);
            assert_eq!(topology.key_prefix.as_deref(), key_prefix);
            assert_eq!(store.uri("manifest.json"), object_uri);
        }
    }

    struct TestS3Server {
        endpoint: String,
        task: tokio::task::JoinHandle<()>,
    }

    impl TestS3Server {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let objects =
                std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::<
                    String,
                    Vec<u8>,
                >::new()));
            let task = tokio::spawn({
                let objects = objects.clone();
                async move {
                    loop {
                        let Ok((mut stream, _)) = listener.accept().await else {
                            break;
                        };
                        let objects = objects.clone();
                        tokio::spawn(async move {
                            let _ = handle_test_s3_connection(&mut stream, objects).await;
                        });
                    }
                }
            });
            Self { endpoint, task }
        }
    }

    impl Drop for TestS3Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn handle_test_s3_connection(
        stream: &mut tokio::net::TcpStream,
        objects: std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>,
    ) -> std::io::Result<()> {
        let mut buffer = Vec::new();
        let header_end = loop {
            let mut chunk = [0u8; 1024];
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Ok(());
            }
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(pos) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                break pos;
            }
        };
        let header_text = String::from_utf8_lossy(&buffer[..header_end]).to_string();
        let mut lines = header_text.lines();
        let request_line = lines.next().unwrap_or_default();
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts.next().unwrap_or_default();
        let target = request_parts.next().unwrap_or_default();
        let headers = lines
            .filter_map(|line| {
                line.split_once(':').map(|(name, value)| {
                    (name.trim().to_ascii_lowercase(), value.trim().to_string())
                })
            })
            .collect::<std::collections::HashMap<_, _>>();
        let content_length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let body_start = header_end + 4;
        while buffer.len() < body_start + content_length {
            let mut chunk = vec![0u8; body_start + content_length - buffer.len()];
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
        let body =
            buffer[body_start..body_start + content_length.min(buffer.len() - body_start)].to_vec();
        let (path, query) = target
            .split_once('?')
            .map_or((target, ""), |(path, query)| (path, query));
        let mut path_parts = path.trim_start_matches('/').splitn(2, '/');
        let bucket = path_parts.next().unwrap_or_default();
        let key = percent_decode(path_parts.next().unwrap_or_default());
        if bucket.is_empty() {
            return write_test_s3_response(stream, 400, &[], b"bucket required").await;
        }
        if method == "GET" && query.contains("list-type=2") {
            let prefix = query
                .split('&')
                .find_map(|part| part.strip_prefix("prefix="))
                .map(percent_decode)
                .unwrap_or_default();
            let objects = objects.lock().await;
            let mut keys = objects
                .keys()
                .filter(|stored_key| stored_key.starts_with(&prefix))
                .cloned()
                .collect::<Vec<_>>();
            keys.sort();
            let mut xml = String::from("<ListBucketResult>");
            for key in keys {
                xml.push_str("<Contents><Key>");
                xml.push_str(&key);
                xml.push_str("</Key></Contents>");
            }
            xml.push_str("</ListBucketResult>");
            return write_test_s3_response(
                stream,
                200,
                &[("Content-Type", "application/xml")],
                xml.as_bytes(),
            )
            .await;
        }
        match method {
            "PUT" => {
                if let Some(copy_source) = headers.get("x-amz-copy-source") {
                    let source_key = percent_decode(
                        copy_source
                            .trim_start_matches('/')
                            .split_once('/')
                            .map_or("", |(_, key)| key),
                    );
                    let source = { objects.lock().await.get(&source_key).cloned() };
                    let Some(source) = source else {
                        return write_test_s3_response(stream, 404, &[], b"not found").await;
                    };
                    objects.lock().await.insert(key, source);
                    write_test_s3_response(stream, 200, &[], b"<CopyObjectResult/>").await
                } else {
                    if headers
                        .get("if-none-match")
                        .is_some_and(|value| value == "*")
                        && objects.lock().await.contains_key(&key)
                    {
                        return write_test_s3_response(stream, 412, &[], b"precondition failed")
                            .await;
                    }
                    objects.lock().await.insert(key, body);
                    write_test_s3_response(stream, 200, &[], b"").await
                }
            }
            "GET" | "HEAD" => {
                let Some(data) = objects.lock().await.get(&key).cloned() else {
                    return write_test_s3_response(stream, 404, &[], b"not found").await;
                };
                let mut data = data;
                let status = if let Some(range) = headers
                    .get("range")
                    .and_then(|value| value.strip_prefix("bytes="))
                {
                    let (start, end) = range.split_once('-').unwrap_or((range, ""));
                    let start = start.parse::<usize>().unwrap_or(0);
                    let end = end
                        .parse::<usize>()
                        .unwrap_or_else(|_| data.len().saturating_sub(1));
                    data = data[start.min(data.len())..=end.min(data.len().saturating_sub(1))]
                        .to_vec();
                    206
                } else {
                    200
                };
                let headers = [("Content-Length", data.len().to_string())];
                let header_refs = [("Content-Length", headers[0].1.as_str())];
                if method == "HEAD" {
                    write_test_s3_response(stream, status, &header_refs, b"").await
                } else {
                    write_test_s3_response(stream, status, &header_refs, &data).await
                }
            }
            "DELETE" => {
                objects.lock().await.remove(&key);
                write_test_s3_response(stream, 204, &[], b"").await
            }
            _ => write_test_s3_response(stream, 405, &[], b"method not allowed").await,
        }
    }

    async fn write_test_s3_response(
        stream: &mut tokio::net::TcpStream,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> std::io::Result<()> {
        let reason = match status {
            200 => "OK",
            204 => "No Content",
            206 => "Partial Content",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            _ => "OK",
        };
        let mut response = format!("HTTP/1.1 {status} {reason}\r\nConnection: close\r\n");
        if !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        {
            response.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        for (name, value) in headers {
            response.push_str(name);
            response.push_str(": ");
            response.push_str(value);
            response.push_str("\r\n");
        }
        response.push_str("\r\n");
        stream.write_all(response.as_bytes()).await?;
        stream.write_all(body).await
    }

    fn percent_decode(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut idx = 0usize;
        while idx < bytes.len() {
            if bytes[idx] == b'%' && idx + 2 < bytes.len() {
                if let Ok(byte) = u8::from_str_radix(&value[idx + 1..idx + 3], 16) {
                    out.push(byte);
                    idx += 3;
                    continue;
                }
            }
            out.push(bytes[idx]);
            idx += 1;
        }
        String::from_utf8_lossy(&out).to_string()
    }
}
