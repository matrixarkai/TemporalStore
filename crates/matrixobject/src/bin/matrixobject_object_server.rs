use matrixobject::{MatrixObjectError, SegmentId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct ObjectService {
    data_path: PathBuf,
    index: Arc<Mutex<BTreeMap<String, ObjectMeta>>>,
    append: Arc<Mutex<AppendState>>,
    tenant_id: String,
    volume_id: String,
    append_segment_id: u64,
}

struct AppendState {
    file: File,
    next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectMeta {
    segment_id: u64,
    #[serde(default)]
    offset: u64,
    len: u64,
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

const LOG_MAGIC: &[u8; 5] = b"MOBJ1";
const LOG_PUT: u8 = 1;
const LOG_DELETE: u8 = 2;
const LOG_HEADER_LEN: usize = 18;

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
    let data_path = root.join("object-data.log");
    let append_segment_id = fnv1a64(b"matrixobject-object-append-v1");
    let index = load_index_from_log(&data_path, append_segment_id).expect("object index should load");
    let data_file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&data_path)
        .expect("object data log should open");
    let file_offset = data_file
        .metadata()
        .expect("object data log metadata should load")
        .len();
    let append_offset = file_offset;
    let service = ObjectService {
        data_path,
        index: Arc::new(Mutex::new(index)),
        append: Arc::new(Mutex::new(AppendState {
            file: data_file,
            next_offset: append_offset,
        })),
        tenant_id,
        volume_id,
        append_segment_id,
    };

    let listener = TcpListener::bind(&bind_addr).expect("failed to bind MATRIXOBJECT_BIND_ADDR");
    eprintln!("matrixobject object server listening on {bind_addr}");
    for stream in listener.incoming() {
        let stream = stream.expect("failed to accept connection");
        let service = service.clone();
        let addr = bind_addr.clone();
        std::thread::spawn(move || {
            let _ = handle_stream(stream, service, addr);
        });
    }
}

fn handle_stream(mut stream: TcpStream, service: ObjectService, addr: String) -> std::io::Result<()> {
    let request = match read_request(&mut stream).and_then(parse_request) {
        Ok(request) => request,
        Err(err) => {
            write_response(&mut stream, 400, &format!(r#"{{"error":"{err}"}}"#))?;
            return Ok(());
        }
    };
    let (status, body) = match route(request, service, addr) {
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
    addr: String,
) -> matrixobject::Result<Vec<u8>> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => json(&HealthResponse {
            ok: true,
            keys: service.index.lock().expect("object index poisoned").len(),
            addr,
        }),
        ("POST", "/v1/object/put_raw") => {
            let req = parse_raw_put(&request.body)?;
            service.put(req)?;
            json(&OkResponse { ok: true })
        }
        ("POST", "/v1/object/put") => {
            let req: PutObjectRequest = serde_json::from_slice(&request.body)?;
            service.put(req)?;
            json(&OkResponse { ok: true })
        }
        ("POST", "/v1/object/get_raw") => {
            let key = parse_raw_key(&request.body)?;
            service.get(key)
        }
        ("POST", "/v1/object/get") => {
            let req: KeyRequest = serde_json::from_slice(&request.body)?;
            let bytes = service.get(req.key)?;
            json(&GetObjectResponse { bytes })
        }
        ("POST", "/v1/object/list") => {
            let req: ListObjectRequest = serde_json::from_slice(&request.body)?;
            let keys = service.list(&req.prefix);
            json(&ListObjectResponse { keys })
        }
        ("POST", "/v1/object/delete") => {
            let req: KeyRequest = serde_json::from_slice(&request.body)?;
            service.delete(req.key)?;
            json(&OkResponse { ok: true })
        }
        _ => Ok(r#"{"error":"not found"}"#.as_bytes().to_vec()),
    }
}

impl ObjectService {
    fn put(&self, req: PutObjectRequest) -> matrixobject::Result<()> {
        validate_key(&req.key)?;
        let segment_id = self.append_segment_id;
        let offset = {
            let mut append = self.append.lock().expect("append state poisoned");
            let next_offset = append.next_offset;
            let offset = append_put_record(&mut append.file, next_offset, &req.key, &req.bytes)?;
            append.file.sync_data()?;
            append.next_offset = append.file.stream_position()?;
            offset
        };
        {
            let mut index = self.index.lock().expect("object index poisoned");
            let key = req.key;
            let meta = ObjectMeta {
                segment_id,
                offset,
                len: req.bytes.len() as u64,
            };
            index.insert(key.clone(), meta.clone());
        }
        Ok(())
    }

    fn get(&self, key: String) -> matrixobject::Result<Vec<u8>> {
        validate_key(&key)?;
        let meta = self
            .index
            .lock()
            .expect("object index poisoned")
            .get(&key)
            .cloned()
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(self.segment_id_for_key(&key)))?;
        let mut file = File::open(&self.data_path)?;
        file.seek(SeekFrom::Start(meta.offset))?;
        let mut bytes = vec![0; meta.len as usize];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
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

    fn delete(&self, key: String) -> matrixobject::Result<()> {
        validate_key(&key)?;
        let meta = self
            .index
            .lock()
            .expect("object index poisoned")
            .remove(&key);
        if meta.is_some() {
            let mut append = self.append.lock().expect("append state poisoned");
            let next_offset = append.next_offset;
            append_delete_record(&mut append.file, next_offset, &key)?;
            append.file.sync_data()?;
            append.next_offset = append.file.stream_position()?;
        }
        Ok(())
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

fn parse_raw_put(body: &[u8]) -> matrixobject::Result<PutObjectRequest> {
    if body.len() < 4 {
        return Err(MatrixObjectError::SharedStore(
            "raw put body missing key length".to_string(),
        ));
    }
    let key_len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
    if body.len() < 4 + key_len {
        return Err(MatrixObjectError::SharedStore(
            "raw put body truncated key".to_string(),
        ));
    }
    let key = std::str::from_utf8(&body[4..4 + key_len])
        .map_err(|err| MatrixObjectError::SharedStore(err.to_string()))?
        .to_string();
    Ok(PutObjectRequest {
        key,
        bytes: body[4 + key_len..].to_vec(),
    })
}

fn parse_raw_key(body: &[u8]) -> matrixobject::Result<String> {
    std::str::from_utf8(body)
        .map(|key| key.to_string())
        .map_err(|err| MatrixObjectError::SharedStore(err.to_string()))
}

fn load_index_from_log(
    path: &PathBuf,
    append_segment_id: u64,
) -> std::io::Result<BTreeMap<String, ObjectMeta>> {
    let mut index = BTreeMap::new();
    let Ok(mut file) = File::open(path) else {
        return Ok(index);
    };
    loop {
        let record_start = file.stream_position()?;
        let mut header = [0; LOG_HEADER_LEN];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        }
        if &header[..LOG_MAGIC.len()] != LOG_MAGIC {
            break;
        }
        let op = header[5];
        let key_len = u32::from_le_bytes(header[6..10].try_into().unwrap()) as usize;
        let value_len = u64::from_le_bytes(header[10..18].try_into().unwrap()) as usize;
        let mut key_bytes = vec![0; key_len];
        if file.read_exact(&mut key_bytes).is_err() {
            break;
        }
        let key = String::from_utf8_lossy(&key_bytes).to_string();
        let value_offset = record_start + LOG_HEADER_LEN as u64 + key_len as u64;
        match op {
            LOG_PUT => {
                if file.seek(SeekFrom::Current(value_len as i64)).is_err() {
                    break;
                }
                index.insert(
                    key,
                    ObjectMeta {
                        segment_id: append_segment_id,
                        offset: value_offset,
                        len: value_len as u64,
                    },
                );
            }
            LOG_DELETE => {
                index.remove(&key);
            }
            _ => break,
        }
    }
    Ok(index)
}

fn append_put_record(file: &mut File, offset: u64, key: &str, bytes: &[u8]) -> std::io::Result<u64> {
    file.seek(SeekFrom::Start(offset))?;
    write_log_header(file, LOG_PUT, key.len() as u32, bytes.len() as u64)?;
    file.write_all(key.as_bytes())?;
    let value_offset = offset + LOG_HEADER_LEN as u64 + key.len() as u64;
    file.write_all(bytes)?;
    Ok(value_offset)
}

fn append_delete_record(file: &mut File, offset: u64, key: &str) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    write_log_header(file, LOG_DELETE, key.len() as u32, 0)?;
    file.write_all(key.as_bytes())
}

fn write_log_header(
    file: &mut File,
    op: u8,
    key_len: u32,
    value_len: u64,
) -> std::io::Result<()> {
    file.write_all(LOG_MAGIC)?;
    file.write_all(&[op])?;
    file.write_all(&key_len.to_le_bytes())?;
    file.write_all(&value_len.to_le_bytes())
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
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
