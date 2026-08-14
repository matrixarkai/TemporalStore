// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
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

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct AppendBlobReceipt {
    pub key: String,
    pub start_offset: u64,
    pub end_offset: u64,
    pub bytes_written: u64,
    pub object_length: u64,
    pub physical_band_count: usize,
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
        let bytes_written = bytes.len() as u64;
        self.put(key, bytes).await?;
        Ok(AppendBlobReceipt {
            key: key.to_string(),
            start_offset: 0,
            end_offset: bytes_written,
            bytes_written,
            object_length: bytes_written,
            physical_band_count: 0,
            first_physical_offset: None,
        })
    }
    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        for (key, bytes) in objects {
            self.put(key, bytes.clone()).await?;
        }
        Ok(())
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

#[derive(Debug, Clone)]
pub struct MatrixObjectHttpStore {
    addr: String,
    uri: String,
    rpc_streams: Arc<Mutex<Vec<TcpStream>>>,
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
            rpc_streams: Arc::default(),
        })
    }
}

#[async_trait]
impl ObjectStore for MatrixObjectHttpStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.rpc_request(RPC_PUT, key, bytes.as_ref())?;
        Ok(())
    }

    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        if objects.is_empty() {
            return Ok(());
        }
        match self.rpc_request(RPC_PUT_MANY, "", &encode_rpc_put_many_request(objects)) {
            Ok(_) => Ok(()),
            Err(_) => {
                for (key, bytes) in objects {
                    self.put(key, bytes.clone()).await?;
                }
                Ok(())
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let response = self.rpc_request(RPC_GET, key, &[])?;
        Ok(Bytes::from(response))
    }

    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        match self.rpc_request(RPC_GET_MANY, "", keys.join("\n").as_bytes()) {
            Ok(response) => parse_rpc_get_many_response(response),
            Err(_) => {
                let mut objects = Vec::with_capacity(keys.len());
                for key in keys {
                    objects.push((key.clone(), self.get(key).await?));
                }
                Ok(objects)
            }
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        match self.rpc_request(RPC_LIST, prefix, &[]) {
            Ok(response) => Ok(parse_rpc_list_response(&response)),
            Err(_) => {
                let response: ListObjectResponse = matrixobject_post(
                    &self.addr,
                    "/v1/object/list",
                    &ListObjectRequest { prefix },
                )?;
                Ok(response.keys)
            }
        }
    }

    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<String>, ObjectStoreError> {
        match self.rpc_request(RPC_LIST_AFTER, prefix, after.as_bytes()) {
            Ok(response) => Ok(parse_rpc_list_response(&response)),
            Err(_) => {
                let response: ListObjectResponse = matrixobject_post(
                    &self.addr,
                    "/v1/object/list",
                    &ListObjectRequest { prefix },
                )?;
                Ok(response
                    .keys
                    .into_iter()
                    .filter(|key| key.as_str() > after)
                    .collect())
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        match self.rpc_request(RPC_DELETE, key, &[]) {
            Ok(_) => Ok(()),
            Err(_) => {
                let response: OkResponse =
                    matrixobject_post(&self.addr, "/v1/object/delete", &KeyRequest { key })?;
                if !response.ok {
                    return Err(ObjectStoreError::Http(
                        "matrixobject delete failed".to_string(),
                    ));
                }
                Ok(())
            }
        }
    }

    fn uri(&self, key: &str) -> String {
        format!("{}/{}", self.uri, key)
    }
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

const RPC_MAGIC: &[u8; 5] = b"MORP1";
const RPC_PUT: u8 = 1;
const RPC_GET: u8 = 2;
const RPC_DELETE: u8 = 3;
const RPC_LIST: u8 = 4;
const RPC_LIST_AFTER: u8 = 5;
const RPC_GET_MANY: u8 = 6;
const RPC_PUT_MANY: u8 = 7;
const RPC_STATUS_OK: u8 = 0;
const RPC_STATUS_NOT_FOUND: u8 = 1;
const RPC_STATUS_ERROR: u8 = 2;
const RPC_REQUEST_HEADER_LEN: usize = 18;
const RPC_RESPONSE_HEADER_LEN: usize = 14;
const RPC_POOL_MAX_IDLE: usize = 32;

impl MatrixObjectHttpStore {
    fn rpc_request(&self, op: u8, key: &str, value: &[u8]) -> Result<Vec<u8>, ObjectStoreError> {
        let mut stream = self.take_rpc_stream()?;
        match rpc_request_on_stream(&mut stream, op, key, value) {
            Ok(response) => {
                self.return_rpc_stream(stream);
                Ok(response)
            }
            Err(first_err) => {
                let mut retry_stream = connect_rpc(&self.addr)?;
                match rpc_request_on_stream(&mut retry_stream, op, key, value) {
                    Ok(response) => {
                        self.return_rpc_stream(retry_stream);
                        Ok(response)
                    }
                    Err(_) => Err(first_err),
                }
            }
        }
    }

    fn take_rpc_stream(&self) -> Result<TcpStream, ObjectStoreError> {
        if let Some(stream) = self
            .rpc_streams
            .lock()
            .expect("matrixobject rpc streams poisoned")
            .pop()
        {
            return Ok(stream);
        }
        connect_rpc(&self.addr)
    }

    fn return_rpc_stream(&self, stream: TcpStream) {
        let mut streams = self
            .rpc_streams
            .lock()
            .expect("matrixobject rpc streams poisoned");
        if streams.len() < RPC_POOL_MAX_IDLE {
            streams.push(stream);
        }
    }
}

fn rpc_request_on_stream(
    stream: &mut TcpStream,
    op: u8,
    key: &str,
    value: &[u8],
) -> Result<Vec<u8>, ObjectStoreError> {
    write_rpc_request(stream, op, key.as_bytes(), value)?;
    read_rpc_response(stream)
}

fn parse_rpc_list_response(response: &[u8]) -> Vec<String> {
    if response.is_empty() {
        return Vec::new();
    }
    String::from_utf8_lossy(response)
        .split('\n')
        .filter(|key| !key.is_empty())
        .map(|key| key.to_string())
        .collect()
}

fn parse_rpc_get_many_response(
    response: Vec<u8>,
) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
    if response.len() < 4 {
        return Err(ObjectStoreError::Http(
            "truncated get_many response count".to_string(),
        ));
    }
    let count = u32::from_le_bytes(response[..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut objects = Vec::with_capacity(count);
    let response = Bytes::from(response);
    for _ in 0..count {
        if response.len().saturating_sub(offset) < 12 {
            return Err(ObjectStoreError::Http(
                "truncated get_many response header".to_string(),
            ));
        }
        let key_len = u32::from_le_bytes(response[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let value_len =
            u64::from_le_bytes(response[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        if response.len().saturating_sub(offset) < key_len + value_len {
            return Err(ObjectStoreError::Http(
                "truncated get_many response body".to_string(),
            ));
        }
        let key = String::from_utf8(response[offset..offset + key_len].to_vec())
            .map_err(|err| ObjectStoreError::Http(err.to_string()))?;
        offset += key_len;
        let value = response.slice(offset..offset + value_len);
        offset += value_len;
        objects.push((key, value));
    }
    Ok(objects)
}

fn encode_rpc_put_many_request(objects: &[(String, Bytes)]) -> Vec<u8> {
    let capacity = 4 + objects
        .iter()
        .map(|(key, bytes)| 12 + key.len() + bytes.len())
        .sum::<usize>();
    let mut body = Vec::with_capacity(capacity);
    body.extend_from_slice(&(objects.len() as u32).to_le_bytes());
    for (key, bytes) in objects {
        body.extend_from_slice(&(key.len() as u32).to_le_bytes());
        body.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        body.extend_from_slice(key.as_bytes());
        body.extend_from_slice(bytes.as_ref());
    }
    body
}

fn connect_rpc(addr: &str) -> Result<TcpStream, ObjectStoreError> {
    let socket_addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| ObjectStoreError::Http(format!("cannot resolve {addr}")))?;
    let stream = TcpStream::connect_timeout(&socket_addr, Duration::from_millis(500))?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    Ok(stream)
}

fn write_rpc_request(
    stream: &mut TcpStream,
    op: u8,
    key: &[u8],
    value: &[u8],
) -> Result<(), ObjectStoreError> {
    let mut header = [0; RPC_REQUEST_HEADER_LEN];
    header[..RPC_MAGIC.len()].copy_from_slice(RPC_MAGIC);
    header[5] = op;
    header[6..10].copy_from_slice(&(key.len() as u32).to_le_bytes());
    header[10..18].copy_from_slice(&(value.len() as u64).to_le_bytes());
    stream.write_all(&header)?;
    stream.write_all(key)?;
    stream.write_all(value)?;
    stream.flush()?;
    Ok(())
}

fn read_rpc_response(stream: &mut TcpStream) -> Result<Vec<u8>, ObjectStoreError> {
    let mut header = [0; RPC_RESPONSE_HEADER_LEN];
    stream.read_exact(&mut header)?;
    if &header[..RPC_MAGIC.len()] != RPC_MAGIC {
        return Err(ObjectStoreError::Http(
            "invalid rpc response magic".to_string(),
        ));
    }
    let status = header[5];
    let body_len = u64::from_le_bytes(header[6..14].try_into().unwrap()) as usize;
    let mut body = vec![0; body_len];
    stream.read_exact(&mut body)?;
    match status {
        RPC_STATUS_OK => Ok(body),
        RPC_STATUS_NOT_FOUND => Err(ObjectStoreError::NotFound(
            String::from_utf8_lossy(&body).to_string(),
        )),
        RPC_STATUS_ERROR => Err(ObjectStoreError::Http(
            String::from_utf8_lossy(&body).to_string(),
        )),
        _ => Err(ObjectStoreError::Http(format!(
            "unknown rpc status {status}"
        ))),
    }
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
        let start_offset = match tokio::fs::metadata(&path).await {
            Ok(metadata) => metadata.len(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
            Err(err) => return Err(ObjectStoreError::Io(err)),
        };
        let bytes_written = bytes.len() as u64;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        let end_offset = start_offset.saturating_add(bytes_written);
        Ok(AppendBlobReceipt {
            key: key.to_string(),
            start_offset,
            end_offset,
            bytes_written,
            object_length: end_offset,
            physical_band_count: 0,
            first_physical_offset: None,
        })
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
