//! Standalone MORP1 matrixobject object-store server for cross-process smoke
//! testing of the networked `matrixobject://` cross-node lazy data-follow path.
//!
//! Speaks the exact same `MORP1` TcpStream request/response framing that
//! `MatrixObjectHttpStore` speaks (lifted verbatim from the in-process mock in
//! `shared_store.rs mod tests`), so a real `server` datanode configured with
//! `TS_SHARED_STORE_URI=matrixobject://host:port` talks to it over real sockets.
//!
//! The in-memory `BTreeMap` is the authoritative store (the server is intended to
//! stay up for the whole test, spanning both nodes). Every stored object is also
//! best-effort mirrored to `--dir` on disk purely as human-inspectable evidence
//! that checkpoints / slabs / WAL entries were uploaded.
//!
//! Usage: objstore_smoke_server <bind_addr> <store_dir>
//!   e.g. objstore_smoke_server 127.0.0.1:17299 /tmp/mo-smoke-store

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const MORP1_MAGIC: &[u8; 5] = b"MORP1";

fn mirror_to_disk(dir: &Path, key: &str, value: &[u8]) {
    // Mirror `key` (which contains '/') into a nested file tree under `dir` so the
    // uploaded objects are visible on disk. Best effort: never fail a request on it.
    let safe: PathBuf = dir.join(key);
    if let Some(parent) = safe.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&safe, value);
}

fn serve_conn(mut stream: TcpStream, store: Arc<Mutex<BTreeMap<String, Vec<u8>>>>, dir: PathBuf) {
    loop {
        let mut header = [0u8; 18];
        if stream.read_exact(&mut header).is_err() {
            return; // client dropped the pooled connection
        }
        if &header[..5] != MORP1_MAGIC {
            return;
        }
        let op = header[5];
        let key_len = u32::from_le_bytes(header[6..10].try_into().unwrap()) as usize;
        let value_len = u64::from_le_bytes(header[10..18].try_into().unwrap()) as usize;
        let mut key_bytes = vec![0u8; key_len];
        if stream.read_exact(&mut key_bytes).is_err() {
            return;
        }
        let mut value = vec![0u8; value_len];
        if stream.read_exact(&mut value).is_err() {
            return;
        }
        let key = String::from_utf8_lossy(&key_bytes).to_string();
        let (status, body) = {
            let mut map = store.lock().unwrap();
            match op {
                1 => {
                    // PUT
                    eprintln!("MORP1 PUT   key={key} bytes={}", value.len());
                    mirror_to_disk(&dir, &key, &value);
                    map.insert(key, value);
                    (0u8, Vec::new())
                }
                2 => match map.get(&key) {
                    // GET
                    Some(bytes) => {
                        eprintln!("MORP1 GET   key={key} -> HIT {} bytes", bytes.len());
                        (0u8, bytes.clone())
                    }
                    None => {
                        eprintln!("MORP1 GET   key={key} -> MISS");
                        (1u8, key.into_bytes())
                    }
                },
                3 => {
                    // DELETE
                    map.remove(&key);
                    (0u8, Vec::new())
                }
                4 => {
                    // LIST prefix
                    let mut keys: Vec<String> =
                        map.keys().filter(|k| k.starts_with(&key)).cloned().collect();
                    keys.sort();
                    eprintln!("MORP1 LIST  prefix={key} -> {} keys", keys.len());
                    (0u8, keys.join("\n").into_bytes())
                }
                5 => {
                    // LIST_AFTER prefix=key, after=value
                    let after = String::from_utf8_lossy(&value).to_string();
                    let mut keys: Vec<String> = map
                        .keys()
                        .filter(|k| k.starts_with(&key) && k.as_str() > after.as_str())
                        .cloned()
                        .collect();
                    keys.sort();
                    (0u8, keys.join("\n").into_bytes())
                }
                6 => {
                    // GET_MANY: value = keys joined by '\n'
                    let mut out = Vec::new();
                    let mut entries = Vec::new();
                    for k in String::from_utf8_lossy(&value)
                        .split('\n')
                        .filter(|k| !k.is_empty())
                    {
                        if let Some(bytes) = map.get(k) {
                            entries.push((k.to_string(), bytes.clone()));
                        }
                    }
                    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                    for (k, v) in entries {
                        out.extend_from_slice(&(k.len() as u32).to_le_bytes());
                        out.extend_from_slice(&(v.len() as u64).to_le_bytes());
                        out.extend_from_slice(k.as_bytes());
                        out.extend_from_slice(&v);
                    }
                    (0u8, out)
                }
                7 => {
                    // PUT_MANY: value = count u32 + [key_len u32, value_len u64, key, value]*
                    if value.len() >= 4 {
                        let count = u32::from_le_bytes(value[0..4].try_into().unwrap()) as usize;
                        let mut off = 4usize;
                        for _ in 0..count {
                            let kl =
                                u32::from_le_bytes(value[off..off + 4].try_into().unwrap()) as usize;
                            off += 4;
                            let vl =
                                u64::from_le_bytes(value[off..off + 8].try_into().unwrap()) as usize;
                            off += 8;
                            let k = String::from_utf8_lossy(&value[off..off + kl]).to_string();
                            off += kl;
                            let v = value[off..off + vl].to_vec();
                            off += vl;
                            mirror_to_disk(&dir, &k, &v);
                            map.insert(k, v);
                        }
                    }
                    (0u8, Vec::new())
                }
                _ => (2u8, b"unknown op".to_vec()),
            }
        };
        let mut resp = Vec::with_capacity(14 + body.len());
        resp.extend_from_slice(MORP1_MAGIC);
        resp.push(status);
        resp.extend_from_slice(&(body.len() as u64).to_le_bytes());
        resp.extend_from_slice(&body);
        if stream.write_all(&resp).is_err() {
            return;
        }
        let _ = stream.flush();
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let bind_addr = args
        .next()
        .or_else(|| std::env::var("MO_SMOKE_BIND").ok())
        .unwrap_or_else(|| "127.0.0.1:17299".to_string());
    let store_dir = args
        .next()
        .or_else(|| std::env::var("MO_SMOKE_DIR").ok())
        .unwrap_or_else(|| "/tmp/mo-smoke-store".to_string());
    let dir = PathBuf::from(&store_dir);
    std::fs::create_dir_all(&dir).expect("create store dir");

    let listener = TcpListener::bind(&bind_addr).expect("bind MORP1 object store");
    let actual = listener.local_addr().expect("local addr");
    println!("objstore_smoke_server listening MORP1 on {actual} dir={store_dir}");
    let store: Arc<Mutex<BTreeMap<String, Vec<u8>>>> = Arc::default();
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let store = Arc::clone(&store);
                let dir = dir.clone();
                std::thread::spawn(move || serve_conn(stream, store, dir));
            }
            Err(err) => {
                eprintln!("accept error: {err}");
                return;
            }
        }
    }
}
