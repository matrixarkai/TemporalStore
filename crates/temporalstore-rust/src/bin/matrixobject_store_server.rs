// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Networked object-store server backed by the real MatrixObject store.
//!
//! Speaks the same wire protocol as `objstore_smoke_server`, so every existing client of
//! `matrixobject://host:port` works unchanged -- but where the smoke server keeps objects in a
//! process-local map with a best-effort disk mirror and no durability at all, this one routes
//! every operation through `MatrixObjectObjectStore` rooted on disk, so what it acknowledges is
//! what a restart serves. Use the smoke server for wiring tests; use this when the numbers or the
//! data matter.
//!
//! Usage mirrors the smoke server: `matrixobject_store_server <bind-addr> <store-dir>`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use temporalstore_rust::matrixobject_store::MatrixObjectObjectStore;
use temporalstore_snapshot::object_store::ObjectStore;

const MORP1_MAGIC: &[u8; 5] = b"MORP1";
const BUCKET: &str = "temporalstore";

fn main() {
    let mut args = std::env::args().skip(1);
    let bind = args.next().unwrap_or_else(|| "0.0.0.0:17200".to_string());
    let dir = args.next().unwrap_or_else(|| "./matrixobject-store".to_string());

    let store = Arc::new(
        MatrixObjectObjectStore::with_persistent_dir(BUCKET, &dir)
            .expect("open the on-disk object store"),
    );
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime"),
    );

    let listener = TcpListener::bind(&bind).expect("bind");
    eprintln!("matrixobject store serving {bind} from {dir}");
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let store = Arc::clone(&store);
        let runtime = Arc::clone(&runtime);
        std::thread::spawn(move || serve_conn(stream, store, runtime));
    }
}

fn serve_conn(
    mut stream: TcpStream,
    store: Arc<MatrixObjectObjectStore>,
    runtime: Arc<tokio::runtime::Runtime>,
) {
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

        let (status, body) = match op {
            // PUT: not acknowledged until the store has committed it.
            1 => match runtime.block_on(store.put(&key, value.into())) {
                Ok(()) => (0u8, Vec::new()),
                Err(err) => (2u8, err.to_string().into_bytes()),
            },
            // GET: status 1 with the key echoed back is the wire's "not found".
            2 => match runtime.block_on(store.get(&key)) {
                Ok(bytes) => (0u8, bytes.to_vec()),
                Err(_) => (1u8, key.into_bytes()),
            },
            3 => match runtime.block_on(store.delete(&key)) {
                Ok(()) => (0u8, Vec::new()),
                Err(err) => (2u8, err.to_string().into_bytes()),
            },
            // LIST prefix -> newline-joined keys, sorted.
            4 => match runtime.block_on(store.list(&key)) {
                Ok(mut keys) => {
                    keys.sort();
                    (0u8, keys.join("\n").into_bytes())
                }
                Err(err) => (2u8, err.to_string().into_bytes()),
            },
            // LIST_AFTER: prefix in the key field, exclusive lower bound in the value field.
            5 => {
                let after = String::from_utf8_lossy(&value).to_string();
                match runtime.block_on(store.list(&key)) {
                    Ok(mut keys) => {
                        keys.retain(|candidate| candidate.as_str() > after.as_str());
                        keys.sort();
                        (0u8, keys.join("\n").into_bytes())
                    }
                    Err(err) => (2u8, err.to_string().into_bytes()),
                }
            }
            // GET_MANY: newline-joined keys in; count + (key_len, value_len, key, value)* out.
            // Absent keys are simply omitted, exactly as the smoke server omits them.
            6 => {
                let mut entries = Vec::new();
                for wanted in String::from_utf8_lossy(&value)
                    .split('\n')
                    .filter(|wanted| !wanted.is_empty())
                {
                    if let Ok(bytes) = runtime.block_on(store.get(wanted)) {
                        entries.push((wanted.to_string(), bytes.to_vec()));
                    }
                }
                let mut out = Vec::new();
                out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for (entry_key, entry_value) in entries {
                    out.extend_from_slice(&(entry_key.len() as u32).to_le_bytes());
                    out.extend_from_slice(&(entry_value.len() as u64).to_le_bytes());
                    out.extend_from_slice(entry_key.as_bytes());
                    out.extend_from_slice(&entry_value);
                }
                (0u8, out)
            }
            // PUT_MANY: count + (key_len, value_len, key, value)* in the value field. All-or-error
            // is not promised by the wire; each object is committed as it lands, like the loop of
            // single PUTs it replaces.
            7 => {
                let mut status = 0u8;
                let mut message = Vec::new();
                let mut off = 4usize;
                let count = if value.len() >= 4 {
                    u32::from_le_bytes(value[0..4].try_into().unwrap()) as usize
                } else {
                    status = 2;
                    message = b"short PUT_MANY body".to_vec();
                    0
                };
                for _ in 0..count {
                    if off + 12 > value.len() {
                        status = 2;
                        message = b"truncated PUT_MANY entry".to_vec();
                        break;
                    }
                    let entry_key_len =
                        u32::from_le_bytes(value[off..off + 4].try_into().unwrap()) as usize;
                    off += 4;
                    let entry_value_len =
                        u64::from_le_bytes(value[off..off + 8].try_into().unwrap()) as usize;
                    off += 8;
                    if off + entry_key_len + entry_value_len > value.len() {
                        status = 2;
                        message = b"truncated PUT_MANY entry".to_vec();
                        break;
                    }
                    let entry_key =
                        String::from_utf8_lossy(&value[off..off + entry_key_len]).to_string();
                    off += entry_key_len;
                    let entry_value = value[off..off + entry_value_len].to_vec();
                    off += entry_value_len;
                    if let Err(err) = runtime.block_on(store.put(&entry_key, entry_value.into())) {
                        status = 2;
                        message = err.to_string().into_bytes();
                        break;
                    }
                }
                (status, message)
            }
            _ => (2u8, b"unknown op".to_vec()),
        };

        let mut resp = Vec::with_capacity(14 + body.len());
        resp.extend_from_slice(MORP1_MAGIC);
        resp.push(status);
        resp.extend_from_slice(&(body.len() as u64).to_le_bytes());
        resp.extend_from_slice(&body);
        if stream.write_all(&resp).is_err() {
            return;
        }
    }
}
