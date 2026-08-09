use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
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
pub struct MatrixObjectHttpStore {
    addr: String,
    uri: String,
}

impl MatrixObjectHttpStore {
    pub fn new(uri: &str) -> Result<Self, ObjectStoreError> {
        let uri = uri.trim();
        let addr = uri
            .strip_prefix("matrixobject://")
            .ok_or_else(|| ObjectStoreError::InvalidKey(uri.to_string()))?
            .trim_end_matches('/')
            .to_string();
        if addr.is_empty() {
            return Err(ObjectStoreError::InvalidKey(uri.to_string()));
        }
        Ok(Self {
            addr,
            uri: uri.trim_end_matches('/').to_string(),
        })
    }
}

#[async_trait]
impl ObjectStore for MatrixObjectHttpStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        let mut body = Vec::with_capacity(4 + key.len() + bytes.len());
        body.extend_from_slice(&(key.len() as u32).to_le_bytes());
        body.extend_from_slice(key.as_bytes());
        body.extend_from_slice(bytes.as_ref());
        let response: OkResponse = serde_json::from_slice(&http_post(
            &self.addr,
            "/v1/object/put_raw",
            &body,
            "application/octet-stream",
        )?)?;
        if !response.ok {
            return Err(ObjectStoreError::Http(
                "matrixobject put failed".to_string(),
            ));
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let response = http_post(
            &self.addr,
            "/v1/object/get_raw",
            key.as_bytes(),
            "application/octet-stream",
        )?;
        Ok(Bytes::from(response))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let response: ListObjectResponse =
            matrixobject_post(&self.addr, "/v1/object/list", &ListObjectRequest { prefix })?;
        Ok(response.keys)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let response: OkResponse =
            matrixobject_post(&self.addr, "/v1/object/delete", &KeyRequest { key })?;
        if !response.ok {
            return Err(ObjectStoreError::Http(
                "matrixobject delete failed".to_string(),
            ));
        }
        Ok(())
    }

    fn uri(&self, key: &str) -> String {
        format!("{}/{}", self.uri, key)
    }
}

#[derive(serde::Serialize)]
struct PutObjectRequest<'a> {
    key: &'a str,
    bytes: &'a [u8],
}

#[derive(serde::Serialize)]
struct KeyRequest<'a> {
    key: &'a str,
}

#[derive(serde::Serialize)]
struct ListObjectRequest<'a> {
    prefix: &'a str,
}

#[derive(serde::Deserialize)]
struct OkResponse {
    ok: bool,
}

#[derive(serde::Deserialize)]
struct ListObjectResponse {
    keys: Vec<String>,
}

fn matrixobject_post<Req, Res>(
    addr: &str,
    path: &str,
    request: &Req,
) -> Result<Res, ObjectStoreError>
where
    Req: serde::Serialize,
    Res: serde::de::DeserializeOwned,
{
    let body = serde_json::to_vec(request)?;
    let raw = http_post(addr, path, &body, "application/json")?;
    let response = serde_json::from_slice(&raw)?;
    Ok(response)
}

fn http_post(
    addr: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
) -> Result<Vec<u8>, ObjectStoreError> {
    let socket_addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| ObjectStoreError::Http(format!("cannot resolve {addr}")))?;
    let mut stream = TcpStream::connect_timeout(&socket_addr, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let marker = b"\r\n\r\n";
    let header_end = response
        .windows(marker.len())
        .position(|window| window == marker)
        .ok_or_else(|| ObjectStoreError::Http("missing response headers".to_string()))?;
    let headers = String::from_utf8_lossy(&response[..header_end]);
    let status_ok = headers
        .lines()
        .next()
        .is_some_and(|line| line.contains(" 200 "));
    let body_start = header_end + marker.len();
    let response_body = response[body_start..].to_vec();
    if !status_ok {
        let text = String::from_utf8_lossy(&response_body);
        if text.contains("not found") {
            return Err(ObjectStoreError::NotFound(path.to_string()));
        }
        return Err(ObjectStoreError::Http(text.to_string()));
    }
    Ok(response_body)
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
