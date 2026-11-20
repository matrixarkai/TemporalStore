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
    /// A durable compare-and-set precondition failed: the object either did not hold the
    /// expected bytes (lost a race / stale owner) or the backend does not support a fenced
    /// conditional put. This is the single-writer FENCE signal — callers MUST treat it as a
    /// hard abort, never a retryable transient error.
    #[error("object store condition failed for {key}: {detail}")]
    ConditionFailed { key: String, detail: String },
    /// A write was sent and neither its success nor its absence could be established.
    ///
    /// The write may or may not be in the object. A caller must NOT repeat it blindly -- for an
    /// append that would store the bytes twice -- and must settle it by reading the object
    /// first.
    #[error("object store outcome unknown for {key}: {detail}")]
    OutcomeUnknown { key: String, detail: String },
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct AppendBlobReceipt {
    pub key: String,
    pub start_offset: u64,
    pub end_offset: u64,
    pub bytes_written: u64,
    pub object_length: u64,
    pub physical_slab_count: usize,
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
        let _ = (key, bytes);
        Err(ObjectStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this object store does not implement append_blob; replacing an object is not an append",
        )))
    }

    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        for (key, bytes) in objects {
            self.put(key, bytes.clone()).await?;
        }
        Ok(())
    }
    /// Durable compare-and-set: atomically replace the object at `key` with `new` iff its
    /// current contents exactly equal `expected` (`None` meaning "the object must not exist").
    /// This is the substrate for a single-writer fence/lease: a stale owner that has been
    /// superseded observes a value it does not expect and is rejected with
    /// [`ObjectStoreError::ConditionFailed`]. The default implementation is FAIL-CLOSED — it
    /// refuses rather than performing an unfenced read-modify-write — so a backend without a
    /// real atomic conditional put can never silently degrade the fence to a racy no-op.
    /// Overridden with a real cross-process atomic CAS by [`FileObjectStore`].
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new: Bytes,
    ) -> Result<(), ObjectStoreError> {
        let _ = (expected, new);
        Err(ObjectStoreError::ConditionFailed {
            key: key.to_string(),
            detail: "compare_and_swap unsupported by this object store backend".to_string(),
        })
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
    /// The bytes of `key` from `offset` to its end, or `None` when the object is SHORTER than
    /// `offset`. At exactly the object's length the answer is `Some` of nothing.
    ///
    /// For a reader that follows an append-only object: it already holds everything before
    /// `offset` and wants only what has been added since, so asking for the whole object --
    /// which grows without bound -- every time it checks is a cost proportional to the log, paid
    /// on every poll. `None` is how a store says the object is not the one the caller was
    /// following any more (it was replaced by something shorter), which the caller must treat as
    /// a reason to start over rather than as an error.
    ///
    /// The default reads the whole object and slices it, which is correct for every store and
    /// leaves the saving to the stores that can range-read.
    async fn get_range_from(
        &self,
        key: &str,
        offset: u64,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let bytes = self.get(key).await?;
        let start = usize::try_from(offset)
            .map_err(|_| ObjectStoreError::InvalidKey(format!("{key}: range offset too large")))?;
        if start > bytes.len() {
            return Ok(None);
        }
        Ok(Some(bytes.slice(start..)))
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

/// One S3 reply from a door that lives in this process.
#[derive(Debug, Clone)]
pub struct DoorReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// An S3 door served IN THIS PROCESS instead of over a socket.
///
/// The direct way to reach a MatrixObject cluster is the gateway's own request handling, run by
/// the node, against the metadata group and the chunk servers: there is no gateway process and
/// no hop to it. Everything above the transport is the code that already talks to a gateway --
/// the append rules, the read-back of an append whose outcome is unknown, ranged reads, listing
/// -- so the two ways cannot disagree about a request, only about how it travels.
///
/// The request is the method, the target as it would appear on the wire (path and query,
/// percent-encoded), the extra headers the store sends, and the body. A transport error is the
/// door failing to answer at all; anything it answers, including a 503, is a [`DoorReply`].
pub trait InProcessDoor: Send + Sync + std::fmt::Debug {
    fn serve(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<DoorReply, String>;
}

/// Networked object store speaking S3 over HTTP to a matrixobject `object_server`.
///
/// # WHY S3, AND WHAT THIS REPLACED
///
/// This type is named for HTTP and spoke neither HTTP nor S3. `put`, `get`, `get_many` and
/// `put_many` framed a protocol of their own -- an 18-byte header beginning `MORP1` and a
/// one-byte opcode -- over a raw socket, while `list`, `list_after` and `delete` posted JSON
/// to `/v1/object/*`. **Nothing shipped served either one.** The only implementation of the
/// binary protocol in this repository was a loopback mock inside a test module, and no server
/// anywhere answers `/v1/object/list`. So `TS_SHARED_STORE_URI=matrixobject://host:port` could
/// only ever point at a test fixture: a networked shared-store write failed on the append the
/// binary protocol had no opcode for, and would have failed on list and delete next.
///
/// matrixobject ships a server that does exist: `object_server`, an S3 front door written on
/// the standard library and on by default. It answers `PutObject`, `GetObject` with `Range`,
/// `DeleteObject` and `ListObjectsV2`, and -- since it learned `x-amz-write-offset-bytes` -- an
/// append. This speaks S3 to that, and the two protocols nobody served are gone.
///
/// # CONNECTIONS ARE REUSED
///
/// Requests go on a pooled keep-alive socket. This began as one connection per request with
/// `Connection: close`, because pooling means framing every reply correctly and that was worth
/// doing deliberately rather than inheriting -- and it turned out to be small, because the door
/// frames bodies with `Content-Length` and refuses `Transfer-Encoding` outright, so there is no
/// chunked decoder to write. What a handshake per request costs is invisible on loopback and a
/// round trip each over a network, twice per shared-store record.
#[derive(Debug, Clone)]
pub struct MatrixObjectHttpStore {
    addr: String,
    /// When set, requests are served by this door in process and no socket is opened.
    door: Option<Arc<dyn InProcessDoor>>,
    uri: String,
    bucket: String,
    /// Object lengths this client last observed, so an append can skip asking.
    ///
    /// # WHY A CACHE IS SAFE HERE, WHEN A CACHED OFFSET NORMALLY WOULD NOT BE
    ///
    /// An append sends the offset it believes is the end, and the server appends THERE or
    /// refuses -- the offset is a condition, not a destination. So a stale entry here cannot
    /// place bytes anywhere wrong; the worst it can do is earn a 412, which the append path
    /// already re-reads and retries. That is what makes this an optimisation rather than a
    /// correctness risk: being wrong costs a round trip, not an object.
    ///
    /// Shared through the `Arc` every caller holds this store behind, so appends from one
    /// process cooperate instead of each paying its own HEAD.
    known_lengths: Arc<Mutex<std::collections::HashMap<String, u64>>>,
    /// How many HTTP requests this store has issued.
    ///
    /// The shared path's cost over a network is REQUESTS, not just bytes: the byte envelope
    /// has been measured down to where its only remaining lever is a batch header, while an
    /// append that asks for the object's length before writing it spends two round trips per
    /// record. Counting them makes that visible and keeps it that way -- a wall-clock figure
    /// on a shared box cannot tell a round trip from a busy neighbour, and this can.
    requests: Arc<std::sync::atomic::AtomicU64>,
    /// Bytes this store has put ON the wire and taken OFF it, headers included.
    ///
    /// Read amplification on the shared path has only ever been measured in process, where
    /// the figure is page fetches against bytes returned. Over a network the quantity that
    /// matters is different and was not measured at all: what a request costs INCLUDING its
    /// head. An S3 GET carries a request line, a Host, a Range and a reply head, and at WAL
    /// record sizes that envelope is not a rounding error -- so it is counted at the wire
    /// rather than inferred from the payload.
    wire_sent: Arc<std::sync::atomic::AtomicU64>,
    wire_received: Arc<std::sync::atomic::AtomicU64>,
    /// Idle sockets kept for reuse.
    ///
    /// Every request used to open its own connection and send `Connection: close`, so the
    /// shared path paid a TCP handshake per request -- two per record, since an entry writes
    /// a WAL frame and an offset-index frame. On loopback that is cheap; over a real network
    /// it is a round trip each, before any byte of the request is read.
    ///
    /// Bounded, because an unbounded pool is a file-descriptor leak wearing a cache's clothes.
    connections: Arc<Mutex<Vec<TcpStream>>>,
    /// How many sockets this store has OPENED.
    ///
    /// The byte counters cannot see what connection reuse buys, because a handshake moves no
    /// HTTP bytes -- reuse took the wire figure from 2.619 to 2.572, which is the
    /// `Connection: close` header no longer being sent and nothing else. What it actually
    /// saved is a TCP round trip per request, invisible on loopback and a full RTT over a
    /// network. This counts it, so the saving is a number rather than an argument.
    connections_opened: Arc<std::sync::atomic::AtomicU64>,
}

/// Idle sockets kept per store. Small on purpose: this client issues requests from one task
/// at a time per object, so a deep pool would hold descriptors open to no benefit.
const MAX_IDLE_CONNECTIONS: usize = 8;

/// Bucket a URI that names none will use.
///
/// `matrixobject://host:port` carries no bucket and every caller so far runs one store per
/// endpoint; `matrixobject://host:port/name` names one explicitly.
pub const DEFAULT_HTTP_BUCKET: &str = "temporalstore";

impl MatrixObjectHttpStore {
    pub fn new(uri: &str) -> Result<Self, ObjectStoreError> {
        let trimmed = uri.trim();
        let rest = trimmed
            .strip_prefix("matrixobject://")
            .ok_or_else(|| ObjectStoreError::InvalidKey(trimmed.to_string()))?;
        let (authority, bucket) = match rest.split_once('/') {
            Some((authority, bucket)) => {
                let bucket = bucket.trim_matches('/');
                (
                    authority,
                    if bucket.is_empty() {
                        DEFAULT_HTTP_BUCKET
                    } else {
                        bucket
                    },
                )
            }
            None => (rest, DEFAULT_HTTP_BUCKET),
        };
        let authority = authority.trim();
        if authority.is_empty() {
            return Err(ObjectStoreError::InvalidKey(trimmed.to_string()));
        }
        Ok(Self {
            addr: authority.to_string(),
            door: None,
            uri: trimmed.trim_end_matches('/').to_string(),
            bucket: bucket.to_string(),
            known_lengths: Arc::default(),
            requests: Arc::default(),
            wire_sent: Arc::default(),
            wire_received: Arc::default(),
            connections: Arc::default(),
            connections_opened: Arc::default(),
        })
    }

    /// A store whose requests are answered by `door` in this process. See [`InProcessDoor`].
    pub fn in_process(door: Arc<dyn InProcessDoor>, bucket: &str) -> Self {
        let bucket = if bucket.trim().is_empty() {
            DEFAULT_HTTP_BUCKET
        } else {
            bucket.trim()
        };
        Self {
            addr: "in-process".to_string(),
            door: Some(door),
            uri: format!("matrixobject://in-process/{bucket}"),
            bucket: bucket.to_string(),
            known_lengths: Arc::default(),
            requests: Arc::default(),
            wire_sent: Arc::default(),
            wire_received: Arc::default(),
            connections: Arc::default(),
            connections_opened: Arc::default(),
        }
    }

    /// Which bucket this endpoint's objects live in.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Synchronous single-object GET for callers outside an async runtime -- the networked
    /// analogue of a plain `std::fs::read`. A sync lazy slab source resolves a checkpoint slab
    /// by address without bridging into an executor, mirroring how the shared-filesystem lazy
    /// source reads the file directly.
    pub fn get_blocking(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let response = self.send("GET", &self.object_target(key), &[], &[])?;
        match response.status {
            200 => Ok(Bytes::from(response.body)),
            404 => Err(ObjectStoreError::NotFound(key.to_string())),
            status => Err(response.failure(status, key)),
        }
    }

    fn object_target(&self, key: &str) -> String {
        format!("/{}/{}", self.bucket, encode_key(key))
    }

    /// How many HTTP requests this store has issued since it was built.
    ///
    /// Counted at the single place requests are sent, so nothing can route around it.
    pub fn requests_issued(&self) -> u64 {
        self.requests.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Bytes written to the socket, request heads included.
    pub fn wire_bytes_sent(&self) -> u64 {
        self.wire_sent.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Bytes read from the socket, response heads included.
    pub fn wire_bytes_received(&self) -> u64 {
        self.wire_received.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many TCP connections this store has opened.
    ///
    /// With reuse this should be far below `requests_issued`; equal to it means every request
    /// is paying a handshake, which is what this client used to do.
    pub fn connections_opened(&self) -> u64 {
        self.connections_opened
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Open a socket and count it. The single door through which this store connects, so a
    /// handshake cannot happen without being counted.
    fn open_connection(&self) -> Result<TcpStream, ObjectStoreError> {
        self.connections_opened
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        connect(&self.addr)
    }

    fn take_connection(&self) -> Option<TcpStream> {
        self.connections
            .lock()
            .expect("matrixobject connections poisoned")
            .pop()
    }

    fn return_connection(&self, stream: TcpStream) {
        let mut pool = self
            .connections
            .lock()
            .expect("matrixobject connections poisoned");
        if pool.len() < MAX_IDLE_CONNECTIONS {
            pool.push(stream);
        }
    }

    fn send(
        &self,
        method: &str,
        target: &str,
        extra: &[(&str, String)],
        body: &[u8],
    ) -> Result<HttpResponse, ObjectStoreError> {
        self.requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if let Some(door) = &self.door {
            // NOTHING WENT ON A WIRE, so nothing is added to the wire counters. A door that
            // fails to answer is a transport failure exactly as a dropped socket is: the
            // request may have been served, and the caller settles that by reading.
            let reply = door
                .serve(method, target, extra, body)
                .map_err(ObjectStoreError::Http)?;
            return Ok(HttpResponse {
                status: reply.status,
                headers: reply.headers,
                body: reply.body,
                raw_len: 0,
                request_len: 0,
                reusable: false,
            });
        }

        // A POOLED SOCKET MAY HAVE DIED WHILE IT SAT IDLE, and nothing tells the holder: the
        // server can close an idle keep-alive connection whenever it likes. So a failure on a
        // reused socket is not an error to report -- it is retried once on a fresh one, and
        // only a failure on a CONNECTION THIS CALL OPENED is real. Without that distinction a
        // perfectly healthy store reports io errors in proportion to how long it was idle.
        // Yields the response directly rather than a nested `Result`: `?` inside the arms
        // keeps the error type unambiguous, where `Ok(..)` in each arm left it for inference
        // to guess between every `From` impl on the error.
        let response = {
            match self.take_connection() {
                Some(mut pooled) => {
                    match http_exchange(&mut pooled, &self.addr, method, target, extra, body) {
                        Ok(response) => {
                            if response.reusable {
                                self.return_connection(pooled);
                            }
                            response
                        }
                        // A POOLED SOCKET MAY HAVE DIED WHILE IT SAT IDLE, and nothing tells
                        // the holder: a server may close an idle keep-alive connection
                        // whenever it likes. So a failure on a REUSED socket is retried once
                        // on a fresh one, and only a failure on a connection this call opened
                        // is real. Without that distinction a healthy store reports io errors
                        // in proportion to how long it was idle.
                        Err(_) => {
                            let mut fresh = self.open_connection()?;
                            let response =
                                http_exchange(&mut fresh, &self.addr, method, target, extra, body)?;
                            if response.reusable {
                                self.return_connection(fresh);
                            }
                            response
                        }
                    }
                }
                None => {
                    let mut fresh = self.open_connection()?;
                    let response =
                        http_exchange(&mut fresh, &self.addr, method, target, extra, body)?;
                    if response.reusable {
                        self.return_connection(fresh);
                    }
                    response
                }
            }
        };
        // COUNTED FROM WHAT THE TRANSPORT ACTUALLY MOVED, not from the payload: the head is
        // most of a small request and all of a HEAD reply, and a figure that leaves it out
        // would report the shared path as cheaper than it is.
        self.wire_sent
            .fetch_add(response.request_len as u64, std::sync::atomic::Ordering::Relaxed);
        self.wire_received
            .fetch_add(response.raw_len as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(response)
    }

    /// The object's current length, or zero when it does not exist yet.
    ///
    /// An append to a key nothing has written is how an append-only object is created, so a
    /// missing key is a length here and not an error.
    fn cached_length(&self, key: &str) -> Option<u64> {
        self.known_lengths
            .lock()
            .expect("matrixobject known lengths poisoned")
            .get(key)
            .copied()
    }

    fn remember_length(&self, key: &str, length: u64) {
        self.known_lengths
            .lock()
            .expect("matrixobject known lengths poisoned")
            .insert(key.to_string(), length);
    }

    /// Drop a remembered length, because something other than an append changed the object.
    ///
    /// A `put` replaces it and a `delete` removes it, and either leaves a remembered end
    /// describing an object that no longer has one. Forgetting costs the next append a HEAD;
    /// not forgetting costs it a refusal and a retry, which is slower and noisier.
    fn forget_length(&self, key: &str) {
        self.known_lengths
            .lock()
            .expect("matrixobject known lengths poisoned")
            .remove(key);
    }

    fn object_length_blocking(&self, key: &str) -> Result<u64, ObjectStoreError> {
        let response = self.send("HEAD", &self.object_target(key), &[], &[])?;
        match response.status {
            200 => Ok(response
                .header("x-amz-object-size")
                .or_else(|| response.header("content-length"))
                .and_then(|value| value.trim().parse::<u64>().ok())
                .unwrap_or(0)),
            404 => Ok(0),
            status => Err(response.failure(status, key)),
        }
    }

    /// `ListObjectsV2`, following continuation tokens.
    ///
    /// Paged rather than assumed to fit: the door answers a bounded number of keys and says so
    /// with `IsTruncated`, and a caller reading only the first page would silently see a prefix
    /// of its own log.
    /// Did `bytes` land at `start_offset` of `key`? A receipt if so, `None` if they are not
    /// there, and `OutcomeUnknown` when the object could not be read to find out.
    ///
    /// An append names the offset it expects and the server appends there or refuses, so bytes
    /// found at that offset are this append's and bytes not found are not in the object. The
    /// object may be longer than the append's end, because later appends follow it.
    fn settle_append(
        &self,
        key: &str,
        start_offset: u64,
        bytes: &Bytes,
    ) -> Result<Option<AppendBlobReceipt>, ObjectStoreError> {
        if bytes.is_empty() {
            return Ok(None);
        }
        let unknown = |detail: String| ObjectStoreError::OutcomeUnknown {
            key: key.to_string(),
            detail,
        };
        let length = bytes.len() as u64;
        let last = start_offset
            .checked_add(length)
            .and_then(|end| end.checked_sub(1))
            .ok_or_else(|| ObjectStoreError::InvalidKey(format!("{key}: range overflow")))?;
        // ASK THE GATEWAY BEFORE READING. A read can say the bytes are in the object, but it cannot
        // say that an attempt still in flight inside a slow server will never commit: reading
        // "absent" and then writing again can store the bytes twice. A resolve answers landed, or
        // absent with the late attempt fenced out, so an absent answer is safe to retry on.
        match self.resolve_append(key, start_offset, bytes)? {
            Some(Resolved::Landed) => return self.landed_receipt(key, start_offset, length),
            Some(Resolved::Absent) => return Ok(None),
            // A gateway that does not know the route: read the object back, as before.
            None => {}
        }
        let response = self
            .send(
                "GET",
                &self.object_target(key),
                &[("range", format!("bytes={start_offset}-{last}"))],
                &[],
            )
            .map_err(|error| unknown(format!("the append could not be read back: {error}")))?;
        let present = match response.status {
            206 => response.body.as_slice() == bytes.as_ref(),
            200 => {
                let start = start_offset as usize;
                response
                    .body
                    .get(start..start + bytes.len())
                    .is_some_and(|found| found == bytes.as_ref())
            }
            404 | 416 => false,
            status => {
                return Err(unknown(format!(
                    "the append could not be read back: the store answered {status}"
                )))
            }
        };
        if !present {
            return Ok(None);
        }
        self.landed_receipt(key, start_offset, length)
    }

    /// The receipt of an append that is known to be in the object: where it landed and how long
    /// the object is now. The object may be longer than the append's end, because later appends
    /// follow it.
    fn landed_receipt(
        &self,
        key: &str,
        start_offset: u64,
        length: u64,
    ) -> Result<Option<AppendBlobReceipt>, ObjectStoreError> {
        let object_length = self.object_length_blocking(key).map_err(|error| {
            ObjectStoreError::OutcomeUnknown {
                key: key.to_string(),
                detail: format!("the append is present but the length could not be read: {error}"),
            }
        })?;
        self.remember_length(key, object_length);
        Ok(Some(AppendBlobReceipt {
            key: key.to_string(),
            start_offset,
            end_offset: start_offset + length,
            bytes_written: length,
            object_length,
            physical_slab_count: 0,
            first_physical_offset: None,
        }))
    }

    /// Ask the gateway whether an append whose reply was lost landed:
    /// `POST /bucket/key?append-resolve&offset=O&length=N&sha256=<hex>`. `landed` means these bytes
    /// are at that offset; `absent` means they are not and no attempt still in flight can put them
    /// there, so a retry as a new append is safe. `None` means the gateway has no such route.
    /// Anything else the gateway says is an undecided outcome, and never a reason to write again.
    fn resolve_append(
        &self,
        key: &str,
        start_offset: u64,
        bytes: &Bytes,
    ) -> Result<Option<Resolved>, ObjectStoreError> {
        use sha2::{Digest, Sha256};
        let unknown = |detail: String| ObjectStoreError::OutcomeUnknown {
            key: key.to_string(),
            detail,
        };
        let digest = hex::encode(Sha256::digest(bytes.as_ref()));
        let target = format!(
            "{}?append-resolve&offset={start_offset}&length={}&sha256={digest}",
            self.object_target(key),
            bytes.len()
        );
        let response = self
            .send("POST", &target, &[], &[])
            .map_err(|error| unknown(format!("the append could not be resolved: {error}")))?;
        match (response.status, response.header("x-append-resolution")) {
            (200, Some(word)) => match word.trim() {
                "landed" => Ok(Some(Resolved::Landed)),
                "absent" => Ok(Some(Resolved::Absent)),
                other => Err(unknown(format!("the gateway resolved the append as {other:?}"))),
            },
            (503, _) => Err(unknown(
                "the gateway could not decide whether the append landed".to_string(),
            )),
            _ => Ok(None),
        }
    }

    fn list_v2_blocking(
        &self,
        prefix: &str,
        after: Option<&str>,
    ) -> Result<Vec<String>, ObjectStoreError> {
        let mut keys: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut target = format!("/{}?list-type=2", self.bucket);
            if !prefix.is_empty() {
                target.push_str("&prefix=");
                target.push_str(&encode_query(prefix));
            }
            if let Some(after) = after {
                target.push_str("&start-after=");
                target.push_str(&encode_query(after));
            }
            if let Some(token) = &token {
                target.push_str("&continuation-token=");
                target.push_str(&encode_query(token));
            }
            let response = self.send("GET", &target, &[], &[])?;
            if response.status != 200 {
                return Err(response.failure(response.status, prefix));
            }
            let body = String::from_utf8_lossy(&response.body);
            keys.extend(xml_values(&body, "Key"));
            let truncated = xml_values(&body, "IsTruncated")
                .first()
                .is_some_and(|value| value.trim() == "true");
            if !truncated {
                break;
            }
            match xml_values(&body, "NextContinuationToken")
                .into_iter()
                .find(|value| !value.is_empty())
            {
                Some(next) => token = Some(next),
                // Truncated with nothing to continue from: stop rather than loop for ever, and
                // say the listing is short instead of pretending it is whole.
                None => {
                    return Err(ObjectStoreError::Http(format!(
                        "the listing of {prefix:?} is truncated and gave no continuation token, \
                         so it cannot be read to the end"
                    )))
                }
            }
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }
}

#[async_trait]
impl ObjectStore for MatrixObjectHttpStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        let response = self.send("PUT", &self.object_target(key), &[], bytes.as_ref())?;
        match response.status {
            200 | 201 | 204 => {
                // A put REPLACES, so any remembered end is now wrong.
                self.remember_length(key, bytes.len() as u64);
                Ok(())
            }
            status => Err(response.failure(status, key)),
        }
    }

    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        // THE OFFSET IS A CONDITION, AND LOSING ON IT IS NORMAL UNDER CONCURRENCY.
        //
        // Appending takes two round trips -- learn the end, then write at it -- and those are
        // not one atomic step, so two writers against the same object can read the same end
        // and one of them loses with a 412. The server is right to refuse: it appends where it
        // was told or not at all, because a write that silently landed elsewhere would leave
        // the caller's own offsets describing bytes that are not there.
        //
        // So the loser re-reads the end and tries again, which is what S3 prescribes for a
        // rejected write offset. That is safe for exactly one reason: **the receipt reports
        // where the bytes LANDED, not where this call first intended to put them.** The WAL's
        // offset index is built from the receipt, so a retried append records its true
        // position and a reader finds its bytes. Reporting the originally intended offset
        // after retrying somewhere else is the thing that would corrupt it.
        //
        // Bounded, and the bound is reported rather than looped past: a writer that keeps
        // losing is contention worth seeing, not something to spin on for ever.
        const APPEND_ATTEMPTS: usize = 8;
        let bytes_written = bytes.len() as u64;
        let mut last_conflict: Option<ObjectStoreError> = None;
        // The offset of an attempt whose outcome is not known: a refused offset (the refusal may
        // be about OUR OWN bytes, landed by a send this client repeated on a fresh connection),
        // a connection that failed after the request left, or a server error. Settled by reading
        // the object BEFORE anything is written again: an offset is a condition, so bytes that
        // are not at it are not in the object, and bytes that are there must not be stored twice.
        let mut unsettled: Option<u64> = None;

        for attempt in 0..APPEND_ATTEMPTS {
            if let Some(start_offset) = unsettled.take() {
                if let Some(receipt) = self.settle_append(key, start_offset, &bytes)? {
                    return Ok(receipt);
                }
            }
            // ONE ROUND TRIP INSTEAD OF TWO, when this client already knows the end. The first
            // attempt may use the cached length; a retry never does, because a retry exists
            // precisely because the end moved.
            let start_offset = match self.cached_length(key) {
                Some(length) if attempt == 0 => length,
                _ => self.object_length_blocking(key)?,
            };
            let response = match self.send(
                "PUT",
                &self.object_target(key),
                &[("x-amz-write-offset-bytes", start_offset.to_string())],
                bytes.as_ref(),
            ) {
                Ok(response) => response,
                Err(error) => {
                    // The request may have been served before the connection failed.
                    self.forget_length(key);
                    unsettled = Some(start_offset);
                    last_conflict = Some(error);
                    continue;
                }
            };
            match response.status {
                200 | 201 | 204 => {
                    // The server reports what the object is NOW. Deriving it from
                    // start + written would paper over a server that appended somewhere else,
                    // which is the one thing the receipt exists to rule out.
                    let object_length = response
                        .header("x-amz-object-size")
                        .and_then(|value| value.trim().parse::<u64>().ok())
                        .ok_or_else(|| {
                            ObjectStoreError::Http(format!(
                                "{key}: the append gave no usable x-amz-object-size, so where the bytes landed is unknown"
                            ))
                        })?;
                    let end_offset = start_offset.saturating_add(bytes_written);
                    if object_length != end_offset {
                        return Err(ObjectStoreError::Http(format!(
                            "{key}: appended {bytes_written} bytes at {start_offset}, so the object should be {end_offset} bytes and the server reports {object_length}"
                        )));
                    }
                    self.remember_length(key, object_length);
                    return Ok(AppendBlobReceipt {
                        key: key.to_string(),
                        start_offset,
                        end_offset,
                        bytes_written,
                        object_length,
                        // NOT KNOWABLE OVER THE WIRE, and reported as absent rather than
                        // guessed -- exactly as `FileObjectStore` reports them.
                        physical_slab_count: 0,
                        first_physical_offset: None,
                    });
                }
                // The end moved -- someone else appended, a remembered length was stale, or this
                // very append landed on an earlier send. Settle before writing again.
                412 => {
                    self.forget_length(key);
                    unsettled = Some(start_offset);
                    last_conflict = Some(response.failure(412, key));
                }
                // A server error says nothing about whether the append was committed.
                500..=599 => {
                    self.forget_length(key);
                    unsettled = Some(start_offset);
                    last_conflict = Some(response.failure(response.status, key));
                }
                status => return Err(response.failure(status, key)),
            }
        }

        // The last attempt may have left something unsettled; a failure must not be reported as
        // "not written" when it was.
        if let Some(start_offset) = unsettled.take() {
            if let Some(receipt) = self.settle_append(key, start_offset, &bytes)? {
                return Ok(receipt);
            }
        }
        Err(last_conflict.unwrap_or_else(|| {
            ObjectStoreError::Http(format!(
                "{key}: gave up appending after {APPEND_ATTEMPTS} attempts"
            ))
        }))
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let response = self.send("GET", &self.object_target(key), &[], &[])?;
        match response.status {
            200 => Ok(Bytes::from(response.body)),
            404 => Err(ObjectStoreError::NotFound(key.to_string())),
            status => Err(response.failure(status, key)),
        }
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, ObjectStoreError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        // OVERRIDDEN RATHER THAN LEFT TO THE TRAIT, whose default fetches the whole object and
        // slices it. A reader of a shared log asks for one record out of a log that grows
        // without bound.
        let last = offset
            .checked_add(length)
            .and_then(|end| end.checked_sub(1))
            .ok_or_else(|| ObjectStoreError::InvalidKey(format!("{key}: range overflow")))?;
        let response = self.send(
            "GET",
            &self.object_target(key),
            &[("range", format!("bytes={offset}-{last}"))],
            &[],
        )?;
        match response.status {
            // 200 means the server ignored the range and sent the whole object, which is a
            // legal answer to any range request -- so slice it here rather than trust the body.
            200 => {
                let start = usize::try_from(offset).map_err(|_| {
                    ObjectStoreError::InvalidKey(format!("{key}: range offset too large"))
                })?;
                let len = usize::try_from(length).map_err(|_| {
                    ObjectStoreError::InvalidKey(format!("{key}: range length too large"))
                })?;
                let end = start
                    .checked_add(len)
                    .ok_or_else(|| ObjectStoreError::InvalidKey(format!("{key}: range overflow")))?;
                if end > response.body.len() {
                    return Err(ObjectStoreError::InvalidKey(format!(
                        "{key}: range {offset}..{} exceeds object length {}",
                        offset.saturating_add(length),
                        response.body.len()
                    )));
                }
                Ok(Bytes::from(response.body).slice(start..end))
            }
            206 => Ok(Bytes::from(response.body)),
            404 => Err(ObjectStoreError::NotFound(key.to_string())),
            status => Err(response.failure(status, key)),
        }
    }

    async fn get_range_from(
        &self,
        key: &str,
        offset: u64,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        // AN OPEN-ENDED RANGE, so one request returns the tail whatever the object's length is.
        let response = self.send(
            "GET",
            &self.object_target(key),
            &[("range", format!("bytes={offset}-"))],
            &[],
        )?;
        match response.status {
            206 => Ok(Some(Bytes::from(response.body))),
            // THE SERVER IGNORED THE RANGE and sent the whole object, which is a legal answer to
            // any range request: slice it here rather than trust the body to be the tail.
            200 => {
                let start = usize::try_from(offset).map_err(|_| {
                    ObjectStoreError::InvalidKey(format!("{key}: range offset too large"))
                })?;
                if start > response.body.len() {
                    return Ok(None);
                }
                Ok(Some(Bytes::from(response.body).slice(start..)))
            }
            // UNSATISFIABLE says the offset is at or past the end, and `Content-Range: bytes */len`
            // says where the end is: AT it is an empty tail, PAST it is an object that is no
            // longer the one being followed. A reply that does not say is treated as the second,
            // because starting over is the safe reading.
            416 => {
                let length = response
                    .header("content-range")
                    .and_then(|value| value.trim().strip_prefix("bytes */"))
                    .and_then(|length| length.trim().parse::<u64>().ok());
                if length == Some(offset) {
                    Ok(Some(Bytes::new()))
                } else {
                    Ok(None)
                }
            }
            404 => Err(ObjectStoreError::NotFound(key.to_string())),
            status => Err(response.failure(status, key)),
        }
    }

    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        let mut objects = Vec::with_capacity(keys.len());
        for key in keys {
            objects.push((key.clone(), self.get(key).await?));
        }
        Ok(objects)
    }

    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        for (key, bytes) in objects {
            self.put(key, bytes.clone()).await?;
        }
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.list_v2_blocking(prefix, None)
    }

    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<String>, ObjectStoreError> {
        // `start-after` is served by the door, so the filtering happens there instead of every
        // key crossing the wire to be dropped here.
        self.list_v2_blocking(prefix, Some(after))
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let response = self.send("DELETE", &self.object_target(key), &[], &[])?;
        match response.status {
            // A delete of something already gone is a success, as S3 has it.
            200 | 202 | 204 | 404 => {
                self.forget_length(key);
                Ok(())
            }
            status => Err(response.failure(status, key)),
        }
    }

    fn uri(&self, key: &str) -> String {
        format!("{}/{}", self.uri, key)
    }
}

/// What a gateway answers when asked to resolve an append whose reply was lost.
enum Resolved {
    /// The bytes are in the object at the offset the append named.
    Landed,
    /// They are not, and no attempt still in flight can put them there.
    Absent,
}

/// One HTTP response, kept whole so a caller can read a header and the body.
struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    /// Bytes read off the socket for this exchange, head included.
    raw_len: usize,
    /// Bytes written to the socket for it, head included.
    request_len: usize,
    /// Whether this socket may be kept for another request.
    reusable: bool,
}

impl HttpResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// The error a non-success status becomes, carrying the server's own words.
    ///
    /// The body is an S3 `<Error>` document and is included: a bare status turns a refused
    /// append into "something went wrong" when the server said exactly what was wrong.
    fn failure(&self, status: u16, what: &str) -> ObjectStoreError {
        if status == 404 {
            return ObjectStoreError::NotFound(what.to_string());
        }
        let text = String::from_utf8_lossy(&self.body);
        let detail = text.trim();
        if detail.is_empty() {
            ObjectStoreError::Http(format!("{what}: the store answered {status}"))
        } else {
            ObjectStoreError::Http(format!("{what}: the store answered {status} -- {detail}"))
        }
    }
}

fn connect(addr: &str) -> Result<TcpStream, ObjectStoreError> {
    let socket_addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| ObjectStoreError::Http(format!("cannot resolve {addr}")))?;
    let stream = TcpStream::connect_timeout(&socket_addr, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// One request and its reply on a socket that may be used again.
///
/// # WHY THIS CAN READ THE BODY BY LENGTH AND NOTHING ELSE
///
/// The door frames every reply with `Content-Length` and refuses `Transfer-Encoding` of any
/// kind with a 501 -- so there is no chunked decoder to get right, which is the thing that
/// makes keep-alive worth doing here rather than an open-ended risk. Reading to end-of-stream
/// is what is no longer possible: the socket stays open, so a read-to-end would block until
/// the server gave up on it.
///
/// A HEAD REPLY CARRIES `Content-Length` AND NO BODY. The door suppresses the body while
/// keeping the header, exactly as HTTP requires, so a reader that believed the header on a
/// HEAD would wait for bytes that are never coming and time out. The method decides, not the
/// header.
fn http_exchange(
    stream: &mut TcpStream,
    addr: &str,
    method: &str,
    target: &str,
    extra: &[(&str, String)],
    body: &[u8],
) -> Result<HttpResponse, ObjectStoreError> {
    let mut head = format!("{method} {target} HTTP/1.1\r\nHost: {addr}\r\n");
    for (name, value) in extra {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes())?;
    if !body.is_empty() {
        stream.write_all(body)?;
    }
    stream.flush()?;

    // Read until the head is complete, keeping whatever body bytes arrived with it.
    let marker = b"\r\n\r\n";
    let mut raw: Vec<u8> = Vec::with_capacity(512);
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(at) = raw
            .windows(marker.len())
            .position(|window| window == marker)
        {
            break at;
        }
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(ObjectStoreError::Http(
                "the store closed the connection before finishing its response head".to_string(),
            ));
        }
        raw.extend_from_slice(&chunk[..read]);
    };

    let head_text = String::from_utf8_lossy(&raw[..header_end]).into_owned();
    let mut lines = head_text.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| ObjectStoreError::Http("unparseable status line".to_string()))?;
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect();

    let header_of = |want: &str| {
        headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(want))
            .map(|(_, value)| value.as_str())
    };

    // A HEAD has no body however long it says it is; 204 and 304 never carry one either.
    let expected = if method.eq_ignore_ascii_case("HEAD") || status == 204 || status == 304 {
        0
    } else {
        header_of("content-length")
            .and_then(|value| value.trim().parse::<usize>().ok())
            .ok_or_else(|| {
                ObjectStoreError::Http(format!(
                    "the store gave no Content-Length on a {status} reply, and this client reads \
                     bodies by length because the door frames them that way"
                ))
            })?
    };

    let body_start = header_end + marker.len();
    while raw.len() - body_start < expected {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(ObjectStoreError::Http(format!(
                "the store closed the connection with {} of {expected} body bytes delivered",
                raw.len() - body_start
            )));
        }
        raw.extend_from_slice(&chunk[..read]);
    }

    // KEEP THE SOCKET ONLY IF THE SERVER SAID SO, and only if the body was read to the byte:
    // a socket with unread bytes on it would hand them to the next request as its reply head.
    let reusable = !header_of("connection").is_some_and(|value| value.eq_ignore_ascii_case("close"))
        && raw.len() - body_start == expected;

    Ok(HttpResponse {
        status,
        headers,
        body: raw[body_start..body_start + expected].to_vec(),
        raw_len: raw.len(),
        request_len: head.len() + body.len(),
        reusable,
    })
}

/// Percent-encode an object key for a request target, LEAVING `/` ALONE.
///
/// A key with slashes in it is one object whose name contains them, exactly as S3 has it: the
/// door takes everything after the bucket as the key. Encoding them would address a different
/// object, and the bug would show only on keys that happen to contain one.
fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' | b':' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Percent-encode a query value, where `/` goes too: it is data here, not structure.
fn encode_query(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Every `<tag>` value in an S3 listing, with the five XML entities put back.
fn xml_values(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(
            after[..end]
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'")
                // LAST, so a literal `&amp;lt;` does not come back as `<`.
                .replace("&amp;", "&"),
        );
        rest = &after[end + close.len()..];
    }
    out
}
/// Where an append-mode write ended, as the file itself reports it.
///
/// On unix the position after a write to a file opened for append is the end of that write,
/// set under the same inode lock as the write, so it stays right while other appenders run.
#[cfg(unix)]
fn landed_end_offset(file: &mut std::fs::File) -> std::io::Result<u64> {
    use std::io::Seek;
    file.stream_position()
}

/// Elsewhere an append-mode handle's position is not defined to follow its writes, so the
/// length is the best available answer. It can be later than this write's end if another
/// appender got in first, which the unix path above does not allow.
#[cfg(not(unix))]
fn landed_end_offset(file: &mut std::fs::File) -> std::io::Result<u64> {
    file.metadata().map(|metadata| metadata.len())
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
            // REMEMBERED ONLY ONCE IT EXISTS. This used to record the directory first and
            // create it after, so a second appender arriving in between saw "already created",
            // skipped the create, and opened a file in a directory that was not there yet.
            // `create_dir_all` is idempotent, so two callers both creating it costs nothing.
            let known = self
                .created_dirs
                .lock()
                .expect("object-store dir cache poisoned")
                .contains(parent);
            if !known {
                tokio::fs::create_dir_all(parent).await?;
                self.created_dirs
                    .lock()
                    .expect("object-store dir cache poisoned")
                    .insert(parent.to_path_buf());
            }
        }
        let bytes_written = bytes.len() as u64;
        // THE OFFSET IS WHERE THE BYTES LANDED, read after the write and never before it.
        //
        // This used to `stat` the file for its length, then open and append, and took the stat
        // as the start. Two appenders that both stat'd before either wrote claimed the same
        // start, while append mode placed each at the real end: receipts that overlapped each
        // other and named ranges holding someone else's bytes.
        //
        // An append-mode write moves the file position to the end of that write as part of the
        // write itself, so the position afterwards is this write's end and the start is that
        // minus its length. The write is one blocking call on a plain `std::fs::File` -- a
        // `tokio::fs::File` splits a large buffer into several writes, and a concurrent append
        // landing between them would put this one's bytes in two places.
        let end_offset = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            file.write_all(&bytes)?;
            file.flush()?;
            landed_end_offset(&mut file)
        })
        .await
        .map_err(|err| ObjectStoreError::Io(std::io::Error::other(err)))??;
        let start_offset = end_offset.saturating_sub(bytes_written);
        Ok(AppendBlobReceipt {
            key: key.to_string(),
            start_offset,
            end_offset,
            bytes_written,
            object_length: end_offset,
            physical_slab_count: 0,
            first_physical_offset: None,
        })
    }

    /// Cross-process atomic compare-and-set. Mutual exclusion is provided by an atomic
    /// `mkdir` of a per-key lock directory (POSIX `mkdir` fails if the directory already
    /// exists, and this atomicity holds over a shared filesystem too), so a concurrent
    /// writer on another host cannot interleave its read-compare-write with ours. Inside the
    /// lock we read the current object, reject with [`ObjectStoreError::ConditionFailed`] if
    /// it does not exactly equal `expected`, then durably (`sync_all`) install `new`.
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new: Bytes,
    ) -> Result<(), ObjectStoreError> {
        let path = self.resolve(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let lock_path = {
            let mut p = path.clone().into_os_string();
            p.push(".caslock");
            PathBuf::from(p)
        };
        let _guard = CasLockGuard::acquire(lock_path).await?;
        let current = match tokio::fs::read(&path).await {
            Ok(bytes) => Some(Bytes::from(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(ObjectStoreError::Io(err)),
        };
        if current.as_deref() != expected.as_deref() {
            return Err(ObjectStoreError::ConditionFailed {
                key: key.to_string(),
                detail: "current object contents do not match expected fence value".to_string(),
            });
        }
        let mut file = tokio::fs::File::create(&path).await?;
        file.write_all(&new).await?;
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

    async fn get_range_from(
        &self,
        key: &str,
        offset: u64,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let path = self.resolve(key)?;
        let mut file = match tokio::fs::File::open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjectStoreError::NotFound(key.to_string()))
            }
            Err(err) => return Err(ObjectStoreError::Io(err)),
        };
        let length = file.metadata().await?.len();
        if offset > length {
            return Ok(None);
        }
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut tail = Vec::with_capacity((length - offset) as usize);
        file.read_to_end(&mut tail).await?;
        Ok(Some(Bytes::from(tail)))
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

/// RAII cross-process lock backed by an atomic `mkdir`. Acquisition spins on `create_dir`
/// (which fails atomically with `AlreadyExists` while another holder owns it) up to a bounded
/// deadline; the directory is removed on drop so the lock is released even on the error path.
struct CasLockGuard {
    lock_path: PathBuf,
}

impl CasLockGuard {
    async fn acquire(lock_path: PathBuf) -> Result<Self, ObjectStoreError> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match tokio::fs::create_dir(&lock_path).await {
                Ok(()) => return Ok(Self { lock_path }),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if std::time::Instant::now() >= deadline {
                        return Err(ObjectStoreError::ConditionFailed {
                            key: lock_path.to_string_lossy().to_string(),
                            detail: "timed out acquiring compare-and-swap lock".to_string(),
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Err(err) => return Err(ObjectStoreError::Io(err)),
            }
        }
    }
}

impl Drop for CasLockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.lock_path);
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

// ---- How a node reaches MatrixObject over the network ----------------------------------

/// The two ways a TemporalStore node can reach a MatrixObject cluster.
///
/// `Gateway` speaks S3 to a gateway (`MatrixObjectHttpStore`). `Direct` talks to the metadata
/// group and the chunk servers itself, with no gateway in between. Both are meant to give the
/// same TemporalStore behaviour and the same durable bytes; the mode only chooses the path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatrixObjectMode {
    Gateway,
    Direct,
}

/// A networked MatrixObject configuration that cannot be used as written.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MatrixObjectModeError {
    #[error("TS_MATRIXOBJECT_MODE={0:?} is not a mode; use \"gateway\" or \"direct\"")]
    UnknownMode(String),
    #[error("gateway mode needs TS_SHARED_STORE_URI=matrixobject://host:port")]
    GatewayNeedsUri,
    #[error("direct mode needs TS_MATRIXOBJECT_META, the metadata group's endpoints")]
    DirectNeedsMeta,
    #[error(
        "both a gateway (TS_SHARED_STORE_URI) and a direct metadata group \
         (TS_MATRIXOBJECT_META) are configured; set TS_MATRIXOBJECT_MODE or remove one, \
         because choosing silently would send the writes to a place the operator did not mean"
    )]
    Ambiguous,
    #[error("TS_MATRIXOBJECT_MODE={mode} conflicts with {conflicting}, which is also set")]
    Conflicting {
        mode: &'static str,
        conflicting: &'static str,
    },
}

/// Pick the mode from the three settings that name it, or refuse. Pure, so every outcome is
/// testable without touching the process environment. An empty or blank value is unset.
///
/// * `mode`: `TS_MATRIXOBJECT_MODE`.
/// * `gateway_uri`: `TS_SHARED_STORE_URI`, which counts only when it names `matrixobject://`.
/// * `direct_meta`: `TS_MATRIXOBJECT_META`.
///
/// `Ok(None)` means no networked MatrixObject is configured at all.
pub fn resolve_matrixobject_mode(
    mode: Option<&str>,
    gateway_uri: Option<&str>,
    direct_meta: Option<&str>,
) -> Result<Option<MatrixObjectMode>, MatrixObjectModeError> {
    fn set(value: Option<&str>) -> Option<&str> {
        value.map(str::trim).filter(|value| !value.is_empty())
    }
    let gateway = set(gateway_uri).filter(|uri| uri.starts_with("matrixobject://"));
    let direct = set(direct_meta);
    match set(mode).map(str::to_ascii_lowercase).as_deref() {
        Some("gateway") => match (gateway, direct) {
            (None, _) => Err(MatrixObjectModeError::GatewayNeedsUri),
            (Some(_), Some(_)) => Err(MatrixObjectModeError::Conflicting {
                mode: "gateway",
                conflicting: "TS_MATRIXOBJECT_META",
            }),
            (Some(_), None) => Ok(Some(MatrixObjectMode::Gateway)),
        },
        Some("direct") => match (direct, gateway) {
            (None, _) => Err(MatrixObjectModeError::DirectNeedsMeta),
            (Some(_), Some(_)) => Err(MatrixObjectModeError::Conflicting {
                mode: "direct",
                conflicting: "TS_SHARED_STORE_URI",
            }),
            (Some(_), None) => Ok(Some(MatrixObjectMode::Direct)),
        },
        Some(other) => Err(MatrixObjectModeError::UnknownMode(other.to_string())),
        None => match (gateway, direct) {
            (Some(_), Some(_)) => Err(MatrixObjectModeError::Ambiguous),
            (Some(_), None) => Ok(Some(MatrixObjectMode::Gateway)),
            (None, Some(_)) => Ok(Some(MatrixObjectMode::Direct)),
            (None, None) => Ok(None),
        },
    }
}

/// A synchronous whole-object read, for the callers that cannot be async (the lazy slab
/// source is consulted from inside a block store's read path).
pub trait BlockingObjectRead: Send + Sync {
    fn get_blocking(&self, key: &str) -> Result<Bytes, ObjectStoreError>;
}

impl BlockingObjectRead for MatrixObjectHttpStore {
    fn get_blocking(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        MatrixObjectHttpStore::get_blocking(self, key)
    }
}

/// The networked MatrixObject store a node holds, whichever way it reaches the cluster.
///
/// Only `Gateway` exists today; `Direct` joins it when the SDK client does. The enum is here
/// first so everything that holds a networked store holds this, and adding the second path is
/// then a change to this file and not to every caller.
///
/// Every `ObjectStore` method is forwarded, the defaulted ones included: a wrapper that omits
/// `append_blob` or `compare_and_swap` compiles and then refuses at run time. The guard test
/// in `shared_store` reads the trait and checks this impl for each method.
#[derive(Debug, Clone)]
pub enum NetworkedMatrixObjectStore {
    Gateway(MatrixObjectHttpStore),
    /// The same store, its requests answered in process by the cluster client.
    Direct(MatrixObjectHttpStore),
}

impl NetworkedMatrixObjectStore {
    /// Connect in the given mode. `target` is the gateway URI for `Gateway`.
    pub fn connect(mode: MatrixObjectMode, target: &str) -> Result<Self, ObjectStoreError> {
        match mode {
            MatrixObjectMode::Gateway => Ok(Self::Gateway(MatrixObjectHttpStore::new(target)?)),
            MatrixObjectMode::Direct => Err(ObjectStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "direct MatrixObject mode needs a binary built with the `matrixobject` feature",
            ))),
        }
    }

    /// The direct way in: a store whose requests `door` answers in this process.
    pub fn direct(door: Arc<dyn InProcessDoor>, bucket: &str) -> Self {
        Self::Direct(MatrixObjectHttpStore::in_process(door, bucket))
    }

    pub fn mode(&self) -> MatrixObjectMode {
        match self {
            Self::Gateway(_) => MatrixObjectMode::Gateway,
            Self::Direct(_) => MatrixObjectMode::Direct,
        }
    }

    pub fn bucket(&self) -> &str {
        match self {
            Self::Gateway(store) => store.bucket(),
            Self::Direct(store) => store.bucket(),
        }
    }

    pub fn requests_issued(&self) -> u64 {
        match self {
            Self::Gateway(store) => store.requests_issued(),
            Self::Direct(store) => store.requests_issued(),
        }
    }

    pub fn wire_bytes_sent(&self) -> u64 {
        match self {
            Self::Gateway(store) => store.wire_bytes_sent(),
            Self::Direct(store) => store.wire_bytes_sent(),
        }
    }

    pub fn wire_bytes_received(&self) -> u64 {
        match self {
            Self::Gateway(store) => store.wire_bytes_received(),
            Self::Direct(store) => store.wire_bytes_received(),
        }
    }

    pub fn connections_opened(&self) -> u64 {
        match self {
            Self::Gateway(store) => store.connections_opened(),
            Self::Direct(store) => store.connections_opened(),
        }
    }
}

impl BlockingObjectRead for NetworkedMatrixObjectStore {
    fn get_blocking(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::Gateway(store) => BlockingObjectRead::get_blocking(store, key),
            Self::Direct(store) => BlockingObjectRead::get_blocking(store, key),
        }
    }
}

#[async_trait]
impl ObjectStore for NetworkedMatrixObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.put(key, bytes).await,
            Self::Direct(store) => store.put(key, bytes).await,
        }
    }

    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.append_blob(key, bytes).await,
            Self::Direct(store) => store.append_blob(key, bytes).await,
        }
    }

    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.put_many(objects).await,
            Self::Direct(store) => store.put_many(objects).await,
        }
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new: Bytes,
    ) -> Result<(), ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.compare_and_swap(key, expected, new).await,
            Self::Direct(store) => store.compare_and_swap(key, expected, new).await,
        }
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.get(key).await,
            Self::Direct(store) => store.get(key).await,
        }
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.get_range(key, offset, length).await,
            Self::Direct(store) => store.get_range(key, offset, length).await,
        }
    }

    async fn get_range_from(
        &self,
        key: &str,
        offset: u64,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.get_range_from(key, offset).await,
            Self::Direct(store) => store.get_range_from(key, offset).await,
        }
    }

    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.get_many(keys).await,
            Self::Direct(store) => store.get_many(keys).await,
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.list(prefix).await,
            Self::Direct(store) => store.list(prefix).await,
        }
    }

    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<String>, ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.list_after(prefix, after).await,
            Self::Direct(store) => store.list_after(prefix, after).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        match self {
            Self::Gateway(store) => store.delete(key).await,
            Self::Direct(store) => store.delete(key).await,
        }
    }

    fn uri(&self, key: &str) -> String {
        match self {
            Self::Gateway(store) => store.uri(key),
            Self::Direct(store) => store.uri(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_put_only_store_refuses_append_without_replacing_bytes() {
        struct PutOnly(FileObjectStore);
        #[async_trait]
        impl ObjectStore for PutOnly {
            async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
                self.0.put(key, bytes).await
            }
            async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
                self.0.get(key).await
            }
            async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
                self.0.list(prefix).await
            }
            async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
                self.0.delete(key).await
            }
            fn uri(&self, key: &str) -> String {
                self.0.uri(key)
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let store = PutOnly(FileObjectStore::new(dir.path()));
        store
            .put("wal", Bytes::from_static(b"first"))
            .await
            .unwrap();
        let error = store
            .append_blob("wal", Bytes::from_static(b"second"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, ObjectStoreError::Io(ref error) if error.kind() == std::io::ErrorKind::Unsupported)
        );
        assert_eq!(
            store.get("wal").await.unwrap(),
            Bytes::from_static(b"first")
        );
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_report_the_offsets_their_bytes_landed_at() {
        // THE RECEIPT HAS TO NAME WHERE THE BYTES LANDED, not where the file ended when the
        // append began. The offset used to be read with a `stat` before the file was opened,
        // so two appenders that stat'd the same length both claimed it, while the append-mode
        // write put each at the real end: receipts that overlapped each other and described
        // bytes that were not there. Every receipt here is checked against the object itself.
        const WRITERS: usize = 8;
        const APPENDS: usize = 60;
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FileObjectStore::new(dir.path()));

        let mut tasks = Vec::new();
        for writer in 0..WRITERS {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                let mut receipts = Vec::new();
                for round in 0..APPENDS {
                    // A byte value and a length unique to the writer, so a range holding
                    // anyone else's bytes cannot pass for this one.
                    let payload = vec![b'a' + writer as u8; 7 + writer * 3 + round % 5];
                    let receipt = store
                        .append_blob("wal/shared", Bytes::from(payload.clone()))
                        .await
                        .unwrap();
                    receipts.push((receipt, payload));
                }
                receipts
            }));
        }
        let mut receipts = Vec::new();
        for task in tasks {
            receipts.extend(task.await.unwrap());
        }

        let object = store.get("wal/shared").await.unwrap();
        let total: u64 = receipts.iter().map(|(_, payload)| payload.len() as u64).sum();
        assert_eq!(object.len() as u64, total, "every appended byte is in the object");

        receipts.sort_by_key(|(receipt, _)| receipt.start_offset);
        let mut expected_start = 0u64;
        for (receipt, payload) in &receipts {
            assert_eq!(
                receipt.start_offset, expected_start,
                "receipts must tile the object: a hole or an overlap means an offset is wrong"
            );
            assert_eq!(receipt.end_offset, expected_start + payload.len() as u64);
            assert_eq!(receipt.bytes_written, payload.len() as u64);
            let landed = &object[receipt.start_offset as usize..receipt.end_offset as usize];
            assert_eq!(landed, payload.as_slice(), "the range a receipt names holds its bytes");
            expected_start = receipt.end_offset;
        }
        assert_eq!(expected_start, total);
    }

    #[tokio::test]
    async fn file_object_store_put_is_reopen_readable_after_durable_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileObjectStore::new(dir.path().join("objects"));

        store
            .put("shards/1/wal/0001.json", Bytes::from_static(b"durable"))
            .await
            .unwrap();

        let reopened = tokio::fs::read(dir.path().join("objects/shards/1/wal/0001.json"))
            .await
            .unwrap();
        assert_eq!(reopened, b"durable");
        assert_eq!(
            store.get("shards/1/wal/0001.json").await.unwrap(),
            Bytes::from_static(b"durable")
        );
    }

    // ---- An append whose outcome is unknown is settled by reading, never repeated blind ----
    //
    // A scripted one-object S3 server. It commits a PUT and THEN misbehaves (drops the socket,
    // answers 503), which is the case a real gateway produces when a consensus deadline passes
    // after the manifest was committed.

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Fault {
        None,
        /// Commit the append, then close the socket without answering.
        CommitThenDrop,
        /// Commit the append, then answer 503.
        CommitThen503,
        /// Answer 503 and commit nothing.
        Refuse503,
    }

    #[derive(Debug)]
    struct ScriptedState {
        data: Vec<u8>,
        existing: bool,
        puts: usize,
        fault_on_first_put: Fault,
        /// Every GET with a range answers 500.
        reads_fail: bool,
    }

    /// What the scripted one-object door answers, as raw HTTP bytes, or `None` to drop the
    /// connection without answering. Shared by the socket server and the in-process door, so
    /// both ways in are served by the same script.
    fn scripted_respond(
        shared: &Mutex<ScriptedState>,
        method: &str,
        header: &dyn Fn(&str) -> Option<String>,
        body: &[u8],
    ) -> Option<Vec<u8>> {
        let reply = |status: &str, extra: &str, body: &[u8], with_body: bool| {
            let mut out = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n",
                body.len()
            )
            .into_bytes();
            if with_body {
                out.extend_from_slice(body);
            }
            out
        };
        let mut state = shared.lock().unwrap();
        let answer: Option<Vec<u8>> = match method {
            "HEAD" if !state.existing => Some(reply("404 Not Found", "", b"", false)),
            "HEAD" => Some(reply(
                "200 OK",
                &format!("x-amz-object-size: {}\r\n", state.data.len()),
                &vec![0u8; state.data.len()],
                false,
            )),
            "GET" => {
                if state.reads_fail {
                    Some(reply("500 Internal Server Error", "", b"read failed", true))
                } else if !state.existing {
                    Some(reply("404 Not Found", "", b"", true))
                } else if let Some(range) = header("range") {
                    let spec = range.trim_start_matches("bytes=");
                    let (first, last) = spec.split_once('-').unwrap();
                    let first: usize = first.parse().unwrap();
                    if first >= state.data.len() {
                        Some(reply(
                            "416 Range Not Satisfiable",
                            &format!("Content-Range: bytes */{}\r\n", state.data.len()),
                            b"",
                            true,
                        ))
                    } else {
                        let last = last
                            .parse::<usize>()
                            .unwrap_or(usize::MAX)
                            .min(state.data.len() - 1);
                        Some(reply("206 Partial Content", "", &state.data[first..=last], true))
                    }
                } else {
                    Some(reply("200 OK", "", &state.data.clone(), true))
                }
            }
            "PUT" => {
                state.puts += 1;
                let fault = if state.puts == 1 { state.fault_on_first_put } else { Fault::None };
                let offset: usize = header("x-amz-write-offset-bytes")
                    .and_then(|value| value.parse().ok())
                    .expect("an append carries its offset");
                if offset != state.data.len() {
                    Some(reply("412 Precondition Failed", "", b"offset", true))
                } else if fault == Fault::Refuse503 {
                    Some(reply("503 Service Unavailable", "", b"try later", true))
                } else {
                    state.data.extend_from_slice(body);
                    state.existing = true;
                    match fault {
                        Fault::CommitThenDrop => None,
                        Fault::CommitThen503 => Some(reply(
                            "503 Service Unavailable",
                            "",
                            b"the publication is unconfirmed",
                            true,
                        )),
                        _ => Some(reply(
                            "200 OK",
                            &format!("x-amz-object-size: {}\r\n", state.data.len()),
                            b"",
                            false,
                        )),
                    }
                }
            }
            _ => Some(reply("405 Method Not Allowed", "", b"", true)),
        };
        drop(state);
        answer
    }

    fn scripted_server(
        fault_on_first_put: Fault,
        reads_fail: bool,
    ) -> (String, Arc<Mutex<ScriptedState>>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(ScriptedState {
            data: Vec::new(),
            existing: false,
            puts: 0,
            fault_on_first_put,
            reads_fail,
        }));
        let shared = state.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut raw = Vec::new();
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    let n = stream.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        break None;
                    }
                    raw.extend_from_slice(&chunk[..n]);
                    if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break Some(at + 4);
                    }
                };
                let Some(head_end) = head_end else { continue };
                let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
                let mut lines = head.lines();
                let request_line = lines.next().unwrap_or_default().to_string();
                let method = request_line.split(' ').next().unwrap_or_default().to_string();
                let header = |name: &str| -> Option<String> {
                    head.lines().find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case(name).then(|| value.trim().to_string())
                    })
                };
                let content_length: usize = header("content-length")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0);
                while raw.len() < head_end + content_length {
                    let n = stream.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&chunk[..n]);
                }
                let body = raw[head_end..].to_vec();
                let answer = scripted_respond(&shared, &method, &header, &body);
                if let Some(answer) = answer {
                    let _ = stream.write_all(&answer);
                }
            }
        });
        (format!("matrixobject://{addr}"), state)
    }

    #[tokio::test]
    async fn an_append_committed_before_the_socket_dropped_is_reported_landed_not_repeated() {
        let (uri, state) = scripted_server(Fault::CommitThenDrop, false);
        let store = MatrixObjectHttpStore::new(&uri).unwrap();
        let receipt = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("the bytes are in the object, so the append succeeded");
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 10));
        assert_eq!(receipt.object_length, 10);
        let state = state.lock().unwrap();
        assert_eq!(state.data, b"record-one", "stored exactly once");
        assert_eq!(state.puts, 1, "nothing was sent a second time");
    }

    #[tokio::test]
    async fn an_append_committed_before_a_503_is_reported_landed_not_repeated() {
        let (uri, state) = scripted_server(Fault::CommitThen503, false);
        let store = MatrixObjectHttpStore::new(&uri).unwrap();
        let receipt = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("a 503 after the commit must be settled by reading");
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 10));
        let state = state.lock().unwrap();
        assert_eq!(state.data, b"record-one", "stored exactly once");
    }

    #[tokio::test]
    async fn an_append_refused_with_a_503_and_not_committed_is_retried_once_it_is_known_absent() {
        let (uri, state) = scripted_server(Fault::Refuse503, false);
        let store = MatrixObjectHttpStore::new(&uri).unwrap();
        let receipt = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("nothing was committed, so the retry is safe and lands");
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 10));
        let state = state.lock().unwrap();
        assert_eq!(state.data, b"record-one");
        assert_eq!(state.puts, 2);
    }

    #[tokio::test]
    async fn an_append_that_cannot_be_read_back_reports_an_unknown_outcome_and_is_not_repeated() {
        let (uri, state) = scripted_server(Fault::CommitThenDrop, true);
        let store = MatrixObjectHttpStore::new(&uri).unwrap();
        let error = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect_err("whether it landed cannot be established");
        assert!(
            matches!(error, ObjectStoreError::OutcomeUnknown { .. }),
            "got {error:?}"
        );
        let state = state.lock().unwrap();
        assert_eq!(state.puts, 1, "an append of unknown outcome is never sent again");
        assert_eq!(state.data, b"record-one");
    }

    // ---- The append resolve ----

    type Answer = Box<dyn Fn(&str, &str, usize) -> Result<DoorReply, String> + Send + Sync>;

    /// A door that answers from a closure (method, target, how many of that method came before)
    /// and records every request it is handed.
    struct CannedDoor {
        log: Mutex<Vec<(String, String)>>,
        answer: Answer,
    }

    impl std::fmt::Debug for CannedDoor {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CannedDoor")
        }
    }

    impl InProcessDoor for CannedDoor {
        fn serve(
            &self,
            method: &str,
            target: &str,
            _headers: &[(&str, String)],
            _body: &[u8],
        ) -> Result<DoorReply, String> {
            let mut log = self.log.lock().unwrap();
            let before = log.iter().filter(|(m, _)| m == method).count();
            log.push((method.to_string(), target.to_string()));
            drop(log);
            (self.answer)(method, target, before)
        }
    }

    fn canned_reply(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Result<DoorReply, String> {
        Ok(DoorReply {
            status,
            headers: headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            body: body.to_vec(),
        })
    }

    fn canned_store(answer: Answer) -> (NetworkedMatrixObjectStore, Arc<CannedDoor>) {
        let door = Arc::new(CannedDoor { log: Mutex::new(Vec::new()), answer });
        (NetworkedMatrixObjectStore::direct(door.clone(), "temporalstore"), door)
    }

    fn count(door: &CannedDoor, method: &str) -> usize {
        door.log.lock().unwrap().iter().filter(|(m, _)| m == method).count()
    }

    #[tokio::test]
    async fn an_append_of_unknown_outcome_is_resolved_by_the_gateway_and_landed_is_a_receipt() {
        // The first length lookup finds nothing; the append is answered 503; the gateway says it
        // landed; the length is then 10. One PUT, one resolve, no read-back.
        let (store, door) = canned_store(Box::new(|method, target, before| match method {
            "HEAD" if before == 0 => canned_reply(404, &[], b""),
            "HEAD" => canned_reply(200, &[("x-amz-object-size", "10")], b""),
            "PUT" => canned_reply(503, &[], b"the publication is unconfirmed"),
            "POST" => {
                assert!(target.starts_with("/temporalstore/wal?append-resolve&offset=0&length=10&sha256="), "{target}");
                canned_reply(200, &[("x-append-resolution", "landed")], b"landed\n")
            }
            other => panic!("unexpected {other} {target}"),
        }));
        let receipt = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("landed is a receipt");
        assert_eq!((receipt.start_offset, receipt.end_offset, receipt.object_length), (0, 10, 10));
        assert_eq!(count(&door, "PUT"), 1, "nothing was sent a second time");
        assert_eq!(count(&door, "POST"), 1);
        assert_eq!(count(&door, "GET"), 0, "a resolve replaces the read-back");
        let log = door.log.lock().unwrap();
        let digest = log.iter().find(|(m, _)| m == "POST").unwrap().1.rsplit('=').next().unwrap().to_string();
        assert_eq!(digest.len(), 64, "the request names the SHA-256 of the bytes");
        assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn an_append_the_gateway_resolves_as_absent_is_retried_as_a_new_append() {
        // Absent means the lost attempt is fenced out, so a second PUT is safe and is the one that lands.
        let (store, door) = canned_store(Box::new(|method, target, before| match method {
            "HEAD" => canned_reply(404, &[], b""),
            "PUT" if before == 0 => canned_reply(503, &[], b"the publication is unconfirmed"),
            "PUT" => canned_reply(200, &[("x-amz-object-size", "10")], b""),
            "POST" => canned_reply(200, &[("x-append-resolution", "absent")], b"absent\n"),
            other => panic!("unexpected {other} {target}"),
        }));
        let receipt = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("absent, then a new append that lands");
        assert_eq!((receipt.start_offset, receipt.end_offset, receipt.object_length), (0, 10, 10));
        assert_eq!(count(&door, "PUT"), 2);
        assert_eq!(count(&door, "POST"), 1);
        assert_eq!(count(&door, "GET"), 0);
    }

    #[tokio::test]
    async fn an_append_the_gateway_cannot_resolve_is_unknown_and_is_not_repeated() {
        let (store, door) = canned_store(Box::new(|method, target, _| match method {
            "HEAD" => canned_reply(404, &[], b""),
            "PUT" => canned_reply(503, &[], b"the publication is unconfirmed"),
            "POST" => canned_reply(503, &[], b"undecided"),
            other => panic!("unexpected {other} {target}"),
        }));
        let error = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect_err("the outcome is undecided");
        assert!(matches!(error, ObjectStoreError::OutcomeUnknown { .. }), "got {error:?}");
        assert_eq!(count(&door, "PUT"), 1, "an undecided append is never sent again");
        assert_eq!(count(&door, "GET"), 0, "and the object is not read back as a substitute");
    }

    #[tokio::test]
    async fn a_gateway_without_the_resolve_route_is_read_back_as_before() {
        let (store, door) = canned_store(Box::new(|method, target, before| match method {
            "HEAD" if before == 0 => canned_reply(404, &[], b""),
            "HEAD" => canned_reply(200, &[("x-amz-object-size", "10")], b""),
            "PUT" => canned_reply(503, &[], b"the publication is unconfirmed"),
            "POST" => canned_reply(405, &[], b""),
            "GET" => canned_reply(206, &[], b"record-one"),
            other => panic!("unexpected {other} {target}"),
        }));
        let receipt = store
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("settled by reading the object");
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 10));
        assert_eq!(count(&door, "PUT"), 1);
        assert_eq!(count(&door, "GET"), 1, "the read-back is the fallback");
    }

    #[tokio::test]
    async fn a_door_that_cannot_answer_is_a_transport_failure_with_its_own_words_and_not_a_503() {
        // On the append path a door failure and a 503 reply are settled the same way, so only a
        // plain read tells them apart: the first carries the door's own words, the second is an
        // answered status.
        let (store, door) = canned_store(Box::new(|method, target, _before| match method {
            "GET" => Err("the door fell over".to_string()),
            other => panic!("unexpected {other} {target}"),
        }));
        let error = store.get("wal").await.expect_err("the door did not answer");
        let text = error.to_string();
        assert!(text.contains("the door fell over"), "the door's own failure is reported: {text}");
        assert!(!text.contains("503"), "a door that fails to answer is not an answered 503: {text}");
        assert_eq!(count(&door, "GET"), 1);
    }

    // ---- The networked store and its mode ----

    #[test]
    fn the_mode_follows_the_settings_and_an_ambiguous_one_is_refused() {
        let gw = Some("matrixobject://host:9000");
        let meta = Some("m1:7000,m2:7000");
        let r = resolve_matrixobject_mode;
        // Nothing set: no networked store, which is today's behaviour byte for byte.
        assert_eq!(r(None, None, None), Ok(None));
        assert_eq!(r(Some(" "), Some(""), Some("  ")), Ok(None));
        // A URI of another scheme is not a gateway.
        assert_eq!(r(None, Some("s3://bucket"), None), Ok(None));
        // One side set picks that side.
        assert_eq!(r(None, gw, None), Ok(Some(MatrixObjectMode::Gateway)));
        assert_eq!(r(None, None, meta), Ok(Some(MatrixObjectMode::Direct)));
        // Both set and no mode: refused, never a silent pick.
        assert_eq!(r(None, gw, meta), Err(MatrixObjectModeError::Ambiguous));
        // An explicit mode must have what it needs and nothing that contradicts it.
        assert_eq!(r(Some("Gateway"), gw, None), Ok(Some(MatrixObjectMode::Gateway)));
        assert_eq!(r(Some("direct"), None, meta), Ok(Some(MatrixObjectMode::Direct)));
        assert_eq!(r(Some("gateway"), None, meta), Err(MatrixObjectModeError::GatewayNeedsUri));
        assert_eq!(r(Some("direct"), gw, None), Err(MatrixObjectModeError::DirectNeedsMeta));
        assert!(matches!(
            r(Some("gateway"), gw, meta),
            Err(MatrixObjectModeError::Conflicting { mode: "gateway", .. })
        ));
        assert!(matches!(
            r(Some("direct"), gw, meta),
            Err(MatrixObjectModeError::Conflicting { mode: "direct", .. })
        ));
        assert!(matches!(
            r(Some("both"), gw, None),
            Err(MatrixObjectModeError::UnknownMode(_))
        ));
    }

    #[test]
    fn direct_mode_is_refused_loudly_until_the_client_is_built_in() {
        let error = NetworkedMatrixObjectStore::connect(MatrixObjectMode::Direct, "m1:7000")
            .expect_err("no direct client yet");
        assert!(error.to_string().contains("`matrixobject` feature"), "{error}");
    }

    #[tokio::test]
    async fn the_networked_store_forwards_every_method_to_the_gateway_store() {
        let (uri, state) = scripted_server(Fault::None, false);
        let store = NetworkedMatrixObjectStore::connect(MatrixObjectMode::Gateway, &uri).unwrap();
        assert_eq!(store.mode(), MatrixObjectMode::Gateway);
        // The defaulted methods: an append and a ranged tail read reach the server and answer
        // as the inner store would, rather than falling back to the trait's refusing default.
        let receipt = store.append_blob("wal", Bytes::from_static(b"abcdef")).await.unwrap();
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 6));
        assert_eq!(store.get_range_from("wal", 2).await.unwrap().unwrap().as_ref(), b"cdef");
        assert_eq!(store.get_range("wal", 1, 3).await.unwrap().as_ref(), b"bcd");
        assert_eq!(BlockingObjectRead::get_blocking(&store, "wal").unwrap().as_ref(), b"abcdef");
        assert_eq!(state.lock().unwrap().data, b"abcdef");
        // compare_and_swap is the one the trait refuses by default; a forwarding wrapper must
        // report the inner store's answer, which for the gateway store is also a refusal -- so
        // check it is the INNER message, not the trait default's.
        let inner = MatrixObjectHttpStore::new(&uri).unwrap();
        let direct = inner.compare_and_swap("k", None, Bytes::from_static(b"x")).await;
        let wrapped = store.compare_and_swap("k", None, Bytes::from_static(b"x")).await;
        assert_eq!(format!("{direct:?}"), format!("{wrapped:?}"));
    }

    /// The scripted door, reached in process: the same answers, no socket.
    #[derive(Debug)]
    struct ScriptedDoor(Arc<Mutex<ScriptedState>>);

    impl InProcessDoor for ScriptedDoor {
        fn serve(
            &self,
            method: &str,
            _target: &str,
            headers: &[(&str, String)],
            body: &[u8],
        ) -> Result<DoorReply, String> {
            let header = |name: &str| -> Option<String> {
                headers
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case(name))
                    .map(|(_, value)| value.clone())
            };
            let raw = scripted_respond(&self.0, method, &header, body)
                .ok_or_else(|| "the door dropped the request".to_string())?;
            let head_end = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("a reply head") + 4;
            let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
            let mut lines = head.lines();
            let status = lines
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|code| code.parse().ok())
                .expect("a status line");
            let headers = lines
                .filter_map(|line| line.split_once(':'))
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .collect();
            Ok(DoorReply {
                status,
                headers,
                body: raw[head_end..].to_vec(),
            })
        }
    }

    fn scripted_direct(
        fault_on_first_put: Fault,
        reads_fail: bool,
    ) -> (NetworkedMatrixObjectStore, Arc<Mutex<ScriptedState>>) {
        let state = Arc::new(Mutex::new(ScriptedState {
            data: Vec::new(),
            existing: false,
            puts: 0,
            fault_on_first_put,
            reads_fail,
        }));
        let store = NetworkedMatrixObjectStore::direct(Arc::new(ScriptedDoor(state.clone())), "temporalstore");
        (store, state)
    }

    fn scripted_gateway(
        fault_on_first_put: Fault,
        reads_fail: bool,
    ) -> (NetworkedMatrixObjectStore, Arc<Mutex<ScriptedState>>) {
        let (uri, state) = scripted_server(fault_on_first_put, reads_fail);
        (NetworkedMatrixObjectStore::connect(MatrixObjectMode::Gateway, &uri).unwrap(), state)
    }

    /// One sequence of writes, run through any store, reporting everything a caller can see.
    async fn drive<S: ObjectStore>(store: &S) -> Vec<String> {
        let mut seen = Vec::new();
        seen.push(format!("absent get: {:?}", store.get("wal").await.map_err(|e| e.to_string())));
        for record in [&b"alpha"[..], b"", b"bravo-bravo", b"c"] {
            let receipt = store.append_blob("wal", Bytes::copy_from_slice(record)).await;
            seen.push(match receipt {
                Ok(r) => format!(
                    "append {} -> start {} end {} written {} length {} slabs {} first {:?}",
                    record.len(),
                    r.start_offset,
                    r.end_offset,
                    r.bytes_written,
                    r.object_length,
                    r.physical_slab_count,
                    r.first_physical_offset
                ),
                Err(e) => format!("append {} -> error {e}", record.len()),
            });
        }
        seen.push(format!("get {:?}", store.get("wal").await.unwrap()));
        seen.push(format!("range 3..8 {:?}", store.get_range("wal", 3, 5).await.unwrap()));
        seen.push(format!("range 0..0 {:?}", store.get_range("wal", 0, 0).await.unwrap()));
        seen.push(format!("range_from 5 {:?}", store.get_range_from("wal", 5).await.unwrap()));
        seen.push(format!("range_from end {:?}", store.get_range_from("wal", 17).await.unwrap()));
        seen.push(format!("range_from past {:?}", store.get_range_from("wal", 18).await.unwrap()));
        seen
    }

    #[tokio::test]
    async fn identical_bytes_in_both_modes() {
        let (gateway, gateway_state) = scripted_gateway(Fault::None, false);
        let (direct, direct_state) = scripted_direct(Fault::None, false);
        assert_eq!(direct.mode(), MatrixObjectMode::Direct);
        let through_gateway = drive(&gateway).await;
        let through_direct = drive(&direct).await;
        // What the caller saw: receipts, reads, ranges, tails.
        assert_eq!(through_gateway, through_direct);
        // What is durably in the object: the bytes themselves.
        assert_eq!(gateway_state.lock().unwrap().data, direct_state.lock().unwrap().data);
        assert_eq!(direct_state.lock().unwrap().data, b"alphabravo-bravoc");
        // The control: the drive wrote and read something, so two empty lists would not pass.
        assert!(through_gateway.len() >= 10, "{through_gateway:?}");
        // The direct way sent nothing over a wire; the gateway way did.
        assert_eq!(direct.wire_bytes_sent(), 0);
        assert!(gateway.wire_bytes_sent() > 0);
    }

    /// The same fault in each way in leaves the same bytes and the same answer.
    #[tokio::test]
    async fn an_append_that_committed_before_a_failure_leaves_the_same_bytes_in_both_modes() {
        for fault in [Fault::CommitThen503, Fault::CommitThenDrop, Fault::Refuse503] {
            let (gateway, gateway_state) = scripted_gateway(fault, false);
            let (direct, direct_state) = scripted_direct(fault, false);
            let a = gateway.append_blob("wal", Bytes::from_static(b"record-one")).await.unwrap();
            let b = direct.append_blob("wal", Bytes::from_static(b"record-one")).await.unwrap();
            assert_eq!(
                (a.start_offset, a.end_offset, a.object_length),
                (b.start_offset, b.end_offset, b.object_length),
                "{fault:?}"
            );
            assert_eq!(gateway_state.lock().unwrap().data, direct_state.lock().unwrap().data, "{fault:?}");
            assert_eq!(direct_state.lock().unwrap().data, b"record-one", "stored exactly once ({fault:?})");
            assert_eq!(
                gateway_state.lock().unwrap().puts,
                direct_state.lock().unwrap().puts,
                "the same number of requests reached the door ({fault:?})"
            );
        }
    }

    #[tokio::test]
    async fn an_append_that_cannot_be_read_back_is_unknown_in_both_modes_and_is_not_repeated() {
        let (gateway, gateway_state) = scripted_gateway(Fault::CommitThenDrop, true);
        let (direct, direct_state) = scripted_direct(Fault::CommitThenDrop, true);
        for (store, state) in [(&gateway, &gateway_state), (&direct, &direct_state)] {
            let error = store
                .append_blob("wal", Bytes::from_static(b"record-one"))
                .await
                .expect_err("whether it landed cannot be established");
            assert!(matches!(error, ObjectStoreError::OutcomeUnknown { .. }), "got {error:?}");
            assert_eq!(state.lock().unwrap().puts, 1, "never sent again");
        }
    }

    /// A long run of sequential appends must not wedge or diverge in either mode: each lands at
    /// the end the last one left, every receipt agrees with the other mode's, and the bytes match.
    #[tokio::test]
    async fn three_thousand_sequential_appends_agree_in_both_modes() {
        const APPENDS: usize = 3000;
        const SIZE: usize = 1024;
        let (gateway, gateway_state) = scripted_gateway(Fault::None, false);
        let (direct, direct_state) = scripted_direct(Fault::None, false);
        for index in 0..APPENDS {
            let record = vec![(index % 251) as u8; SIZE];
            let a = gateway
                .append_blob("wal", Bytes::from(record.clone()))
                .await
                .unwrap_or_else(|e| panic!("gateway append {index}: {e}"));
            let b = direct
                .append_blob("wal", Bytes::from(record))
                .await
                .unwrap_or_else(|e| panic!("direct append {index}: {e}"));
            assert_eq!(
                (a.start_offset, a.end_offset, a.object_length),
                (b.start_offset, b.end_offset, b.object_length),
                "receipts diverged at append {index}"
            );
            assert_eq!(a.start_offset, (index * SIZE) as u64, "append {index} landed off the end");
        }
        assert_eq!(direct_state.lock().unwrap().data.len(), APPENDS * SIZE);
        assert!(
            gateway_state.lock().unwrap().data == direct_state.lock().unwrap().data,
            "the stored bytes differ"
        );
        // One request per append once the length is known: neither way pays a HEAD each time.
        assert_eq!(direct.requests_issued() as usize, APPENDS + 1);
        assert_eq!(gateway.requests_issued() as usize, APPENDS + 1);
    }

    /// A door that fails to answer is a transport failure, settled by reading and not repeated.
    #[tokio::test]
    async fn a_door_that_fails_to_answer_is_settled_by_reading_the_object() {
        let (direct, state) = scripted_direct(Fault::CommitThenDrop, false);
        let receipt = direct
            .append_blob("wal", Bytes::from_static(b"record-one"))
            .await
            .expect("the bytes are in the object, so the append succeeded");
        assert_eq!((receipt.start_offset, receipt.end_offset), (0, 10));
        assert_eq!(state.lock().unwrap().puts, 1, "nothing was sent a second time");
    }
}
