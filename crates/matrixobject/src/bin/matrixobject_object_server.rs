use bytes::Bytes;
use matrixobject::{
    ClientDesc, LocalMatrixObjectStore, MatrixObjectConfig, MatrixObjectError, OpenSegmentRequest,
    QoSRequest, RawSegmentReadRequest, RawSegmentWriteRequest, SegmentId, WriteDurability,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

#[derive(Clone)]
struct ObjectService {
    store: Arc<LocalMatrixObjectStore>,
    index_path: PathBuf,
    journal_path: PathBuf,
    index: Arc<Mutex<BTreeMap<String, ObjectMeta>>>,
    append_offset: Arc<Mutex<u64>>,
    tenant_id: String,
    volume_id: String,
    append_segment_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectMeta {
    segment_id: u64,
    #[serde(default)]
    offset: u64,
    len: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum ObjectIndexRecord {
    Put { key: String, meta: ObjectMeta },
    Delete { key: String },
}

#[derive(Deserialize)]
struct PutObjectRequest {
    key: String,
    bytes: Vec<u8>,
}

#[derive(Deserialize)]
struct KeyRequest {
    key: String,
}

#[derive(Deserialize)]
struct ListObjectRequest {
    prefix: String,
}

#[derive(Serialize)]
struct OkResponse {
    ok: bool,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    keys: usize,
    addr: String,
}

#[derive(Serialize)]
struct GetObjectResponse {
    bytes: Vec<u8>,
}

#[derive(Serialize)]
struct ListObjectResponse {
    keys: Vec<String>,
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

fn main() {
    let bind_addr =
        std::env::var("MATRIXOBJECT_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:18080".to_string());
    let root = PathBuf::from(
        std::env::var("MATRIXOBJECT_ROOT").unwrap_or_else(|_| "/var/lib/matrixobject".to_string()),
    );
    let tenant_id =
        std::env::var("MATRIXOBJECT_TENANT_ID").unwrap_or_else(|_| "temporalstore".to_string());
    let volume_id =
        std::env::var("MATRIXOBJECT_VOLUME_ID").unwrap_or_else(|_| "shared-store".to_string());

    std::fs::create_dir_all(&root).expect("failed to create MATRIXOBJECT_ROOT");
    let runtime = Arc::new(Runtime::new().expect("tokio runtime should start"));
    let store = runtime
        .block_on(LocalMatrixObjectStore::open(MatrixObjectConfig::new(
            root.join("data"),
        )))
        .expect("matrixobject store should open");
    let index_path = root.join("object-index.json");
    let journal_path = root.join("object-index.journal.jsonl");
    let index = load_index(&index_path, &journal_path).expect("object index should load");
    let append_segment_id = fnv1a64(b"matrixobject-object-append-v1");
    let append_offset = index
        .values()
        .filter(|meta| meta.segment_id == append_segment_id)
        .map(|meta| meta.offset.saturating_add(meta.len))
        .max()
        .unwrap_or_default();
    let service = ObjectService {
        store: Arc::new(store),
        index_path,
        journal_path,
        index: Arc::new(Mutex::new(index)),
        append_offset: Arc::new(Mutex::new(append_offset)),
        tenant_id,
        volume_id,
        append_segment_id,
    };

    let listener = TcpListener::bind(&bind_addr).expect("failed to bind MATRIXOBJECT_BIND_ADDR");
    eprintln!("matrixobject object server listening on {bind_addr}");
    for stream in listener.incoming() {
        let stream = stream.expect("failed to accept connection");
        let service = service.clone();
        let runtime = Arc::clone(&runtime);
        let addr = bind_addr.clone();
        std::thread::spawn(move || {
            let _ = handle_stream(stream, service, runtime, addr);
        });
    }
}

fn handle_stream(
    mut stream: TcpStream,
    service: ObjectService,
    runtime: Arc<Runtime>,
    addr: String,
) -> std::io::Result<()> {
    let request = match read_request(&mut stream).and_then(parse_request) {
        Ok(request) => request,
        Err(err) => {
            write_response(&mut stream, 400, &format!(r#"{{"error":"{err}"}}"#))?;
            return Ok(());
        }
    };
    let (status, body) = match route(request, service, runtime, addr) {
        Ok(body) => (200, body),
        Err(MatrixObjectError::SegmentNotFound(_)) => {
            (404, r#"{"error":"object not found"}"#.as_bytes().to_vec())
        }
        Err(err) => (500, json_error(&err.to_string())),
    };
    write_response(&mut stream, status, &String::from_utf8_lossy(&body))
}

fn route(
    request: HttpRequest,
    service: ObjectService,
    runtime: Arc<Runtime>,
    addr: String,
) -> matrixobject::Result<Vec<u8>> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => json(&HealthResponse {
            ok: true,
            keys: service.index.lock().expect("object index poisoned").len(),
            addr,
        }),
        ("POST", "/v1/object/put") => {
            let req: PutObjectRequest = serde_json::from_slice(&request.body)?;
            runtime.block_on(service.put(req))?;
            json(&OkResponse { ok: true })
        }
        ("POST", "/v1/object/get") => {
            let req: KeyRequest = serde_json::from_slice(&request.body)?;
            let bytes = runtime.block_on(service.get(req.key))?;
            json(&GetObjectResponse { bytes })
        }
        ("POST", "/v1/object/list") => {
            let req: ListObjectRequest = serde_json::from_slice(&request.body)?;
            let keys = service.list(&req.prefix);
            json(&ListObjectResponse { keys })
        }
        ("POST", "/v1/object/delete") => {
            let req: KeyRequest = serde_json::from_slice(&request.body)?;
            runtime.block_on(service.delete(req.key))?;
            json(&OkResponse { ok: true })
        }
        _ => Ok(r#"{"error":"not found"}"#.as_bytes().to_vec()),
    }
}

impl ObjectService {
    async fn put(&self, req: PutObjectRequest) -> matrixobject::Result<()> {
        validate_key(&req.key)?;
        let segment_id = self.append_segment_id;
        let offset = {
            let mut append_offset = self.append_offset.lock().expect("append offset poisoned");
            let offset = *append_offset;
            *append_offset = append_offset.saturating_add(req.bytes.len() as u64);
            offset
        };
        let segment = SegmentId::new(&self.tenant_id, &self.volume_id, segment_id);
        self.store
            .open_segment(OpenSegmentRequest {
                segment_id: segment.clone(),
                expected_open_version: None,
                create_if_missing: true,
                client: ClientDesc::default(),
            })
            .await?;
        self.store
            .raw_write(RawSegmentWriteRequest {
                segment_id: segment,
                offset,
                data: Bytes::from(req.bytes.clone()),
                durability: WriteDurability::SyncAll,
                sequence_id: now_micros(),
                open_version: None,
                client: ClientDesc::default(),
                qos: QoSRequest::default(),
            })
            .await?;
        {
            let mut index = self.index.lock().expect("object index poisoned");
            let key = req.key;
            let meta = ObjectMeta {
                segment_id,
                offset,
                len: req.bytes.len() as u64,
            };
            index.insert(key.clone(), meta.clone());
            append_index_record(&self.journal_path, &ObjectIndexRecord::Put { key, meta })?;
        }
        Ok(())
    }

    async fn get(&self, key: String) -> matrixobject::Result<Vec<u8>> {
        validate_key(&key)?;
        let meta = self
            .index
            .lock()
            .expect("object index poisoned")
            .get(&key)
            .cloned()
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(self.segment_id_for_key(&key)))?;
        let response = self
            .store
            .raw_read(RawSegmentReadRequest {
                segment_id: SegmentId::new(&self.tenant_id, &self.volume_id, meta.segment_id),
                offset: meta.offset,
                length: meta.len,
                sequence_id: now_micros(),
                open_version: None,
                client: ClientDesc::default(),
                qos: QoSRequest::default(),
            })
            .await?;
        Ok(response.data.to_vec())
    }

    fn list(&self, prefix: &str) -> Vec<String> {
        let mut keys = self
            .index
            .lock()
            .expect("object index poisoned")
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    async fn delete(&self, key: String) -> matrixobject::Result<()> {
        validate_key(&key)?;
        let meta = self
            .index
            .lock()
            .expect("object index poisoned")
            .remove(&key);
        if meta.is_some() {
            let index = self.index.lock().expect("object index poisoned");
            append_index_record(&self.journal_path, &ObjectIndexRecord::Delete { key })?;
            if self.should_compact_index(&index) {
                save_index(&self.index_path, &index)?;
                reset_index_journal(&self.journal_path)?;
            }
        }
        Ok(())
    }

    fn should_compact_index(&self, index: &BTreeMap<String, ObjectMeta>) -> bool {
        let Ok(journal) = std::fs::metadata(&self.journal_path) else {
            return false;
        };
        let snapshot_size = std::fs::metadata(&self.index_path)
            .map(|meta| meta.len())
            .unwrap_or_default();
        journal.len() > 8 * 1024 * 1024
            || (index.is_empty() && journal.len() > snapshot_size.saturating_mul(4).max(1))
    }

    fn segment_id_for_key(&self, key: &str) -> SegmentId {
        SegmentId::new(&self.tenant_id, &self.volume_id, fnv1a64(key.as_bytes()))
    }
}

fn validate_key(key: &str) -> matrixobject::Result<()> {
    if key.is_empty() || key.contains("..") || key.starts_with('/') || key.starts_with('\\') {
        return Err(MatrixObjectError::SharedStore(format!(
            "invalid object key {key:?}"
        )));
    }
    Ok(())
}

fn load_index(
    path: &PathBuf,
    journal_path: &PathBuf,
) -> std::io::Result<BTreeMap<String, ObjectMeta>> {
    let mut index = if path.exists() {
        let bytes = std::fs::read(path)?;
        serde_json::from_slice(&bytes).unwrap_or_default()
    } else {
        BTreeMap::new()
    };
    if journal_path.exists() {
        let bytes = std::fs::read(journal_path)?;
        for line in bytes.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<ObjectIndexRecord>(line) {
                Ok(ObjectIndexRecord::Put { key, meta }) => {
                    index.insert(key, meta);
                }
                Ok(ObjectIndexRecord::Delete { key }) => {
                    index.remove(&key);
                }
                Err(_) => {}
            }
        }
    }
    Ok(index)
}

fn save_index(path: &PathBuf, index: &BTreeMap<String, ObjectMeta>) -> std::io::Result<()> {
    let temp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(index).map_err(std::io::Error::other)?;
    std::fs::write(&temp, bytes)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

fn append_index_record(path: &PathBuf, record: &ObjectIndexRecord) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut file, record).map_err(std::io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

fn reset_index_journal(path: &PathBuf) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn json<T: Serialize>(value: &T) -> matrixobject::Result<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

fn json_error(message: &str) -> Vec<u8> {
    serde_json::json!({ "error": message })
        .to_string()
        .into_bytes()
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut chunk = [0; 4096];
    let mut expected_len = None;
    loop {
        let size = stream.read(&mut chunk)?;
        if size == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..size]);
        if expected_len.is_none() {
            if let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&buffer[..header_end]);
                let content_len = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                    .unwrap_or_default();
                expected_len = Some(header_end + 4 + content_len);
            }
        }
        if expected_len.is_some_and(|len| buffer.len() >= len) {
            break;
        }
    }
    Ok(buffer)
}

fn parse_request(bytes: Vec<u8>) -> std::io::Result<HttpRequest> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| std::io::Error::other("missing header terminator"))?;
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = headers.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| std::io::Error::other("missing request line"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    Ok(HttpRequest {
        method,
        path,
        body: bytes[header_end + 4..].to_vec(),
    })
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let status_text = if status == 200 { "OK" } else { "ERROR" };
    write!(
        stream,
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}
