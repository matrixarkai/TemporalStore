// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Engine-owned attachment blob store.
//!
//! A resource whose payload is too large to inline in records needs somewhere real to live so
//! one TemporalStore can hold everything: the chunks carry searchable text, and the FULL
//! original attachment is fetchable again from here. Blobs are files under the engine's own
//! durable directory -- `<index_dir>/resources/<tenant>/<content-hash>.bin` -- written through a
//! staging file and published by rename, so a crash mid-upload leaves only staging garbage that
//! the sweep collects, never a half-visible blob. Blobs deliberately do NOT ride the WAL or the
//! block store: they are immutable, content-addressed, and often orders of magnitude larger
//! than a record, so replaying or compacting them would only amplify every maintenance pass.
//!
//! The manifest record (the resource record carrying `external_object_uri`) is the commit
//! point: a blob file with no manifest is unreachable garbage; a manifest whose blob is missing
//! is an error surfaced at fetch time. The sweep takes the referenced set from the caller (who
//! can read the manifests) and only deletes unreferenced files older than a minimum age, so an
//! upload racing the sweep is never collected out from under its not-yet-written manifest.
//!
//! On a raft-replicated deployment these commands execute on every replica during apply, so
//! each replica holds its own copy of the blob under its own directory -- fetches are local
//! everywhere. Upload tokens are generated per-process; multi-part uploads must be driven
//! against one node (the proxy already pins a client to a node for a session).

use super::*;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use std::io::{Read as _, Seek as _};
use std::sync::atomic::{AtomicU64, Ordering};

const RESOURCE_BLOB_URI_PREFIX: &str = "temporalstore://resources/";
const STAGING_DIR: &str = ".staging";

static UPLOAD_COUNTER: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `temporalstore://resources/{tenant:016x}/{hash:016x}` -- the only URI shape this store
/// serves. Parsing is strict: both segments must be exactly 16 lowercase hex digits, so a URI
/// can never smuggle a path.
pub fn format_resource_blob_uri(tenant_hash: u64, content_hash: u64) -> String {
    format!("{RESOURCE_BLOB_URI_PREFIX}{tenant_hash:016x}/{content_hash:016x}")
}

pub fn parse_resource_blob_uri(uri: &str) -> Option<(u64, u64)> {
    let rest = uri.strip_prefix(RESOURCE_BLOB_URI_PREFIX)?;
    let (tenant, hash) = rest.split_once('/')?;
    if tenant.len() != 16 || hash.len() != 16 {
        return None;
    }
    let tenant_hash = u64::from_str_radix(tenant, 16).ok()?;
    let content_hash = u64::from_str_radix(hash, 16).ok()?;
    // Round-trip check rejects uppercase or otherwise non-canonical spellings.
    if format!("{tenant_hash:016x}") != tenant || format!("{content_hash:016x}") != hash {
        return None;
    }
    Some((tenant_hash, content_hash))
}

fn valid_upload_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 64
        && token
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

impl TemporalEngine {
    fn resource_blob_root(&self) -> PathBuf {
        self.index_dir.join("resources")
    }

    fn resource_blob_path(&self, tenant_hash: u64, content_hash: u64) -> PathBuf {
        self.resource_blob_root()
            .join(format!("{tenant_hash:016x}"))
            .join(format!("{content_hash:016x}.bin"))
    }

    fn resource_blob_staging_path(&self, token: &str) -> PathBuf {
        self.resource_blob_root()
            .join(STAGING_DIR)
            .join(format!("{token}.part"))
    }

    /// Start a multi-part upload: create an empty staging file and hand back its token.
    pub fn resource_blob_begin(&self, tenant_hash: u64) -> std::io::Result<String> {
        let seq = UPLOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
        let token = format!(
            "u{:x}-{:x}-{:x}-{seq:x}",
            std::process::id(),
            tenant_hash,
            now_ms(),
        );
        let path = self.resource_blob_staging_path(&token);
        std::fs::create_dir_all(path.parent().expect("staging path has a parent"))?;
        std::fs::File::create(&path)?;
        Ok(token)
    }

    /// Append one part to a staged upload. Parts are appended in call order; the caller drives
    /// parts sequentially against one node.
    pub fn resource_blob_append(&self, token: &str, bytes: &[u8]) -> std::io::Result<u64> {
        if !valid_upload_token(token) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid upload token: {token:?}"),
            ));
        }
        let path = self.resource_blob_staging_path(token);
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        file.write_all(bytes)?;
        Ok(file.metadata()?.len())
    }

    /// Publish a staged upload: hash the staged bytes, fsync, rename to the content-addressed
    /// path, fsync the directory. Returns (uri, size, content_hash). Publishing the same
    /// content twice lands on the same path -- the second rename simply replaces an identical
    /// file.
    pub fn resource_blob_commit(
        &self,
        tenant_hash: u64,
        token: &str,
    ) -> std::io::Result<(String, u64, u64)> {
        if !valid_upload_token(token) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid upload token: {token:?}"),
            ));
        }
        let staged = self.resource_blob_staging_path(token);
        let mut file = std::fs::File::open(&staged)?;
        let mut hash = hashing::stable_object_hash_begin();
        let mut size = 0u64;
        let mut buffer = vec![0u8; 256 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hashing::stable_object_hash_update(&mut hash, &buffer[..read]);
            size += read as u64;
        }
        // Mix the length in so a truncation of a repeating payload cannot collide with the
        // original.
        hashing::stable_object_hash_update_u64_decimal(&mut hash, size);
        drop(file);
        let final_path = self.resource_blob_path(tenant_hash, hash);
        std::fs::create_dir_all(final_path.parent().expect("blob path has a parent"))?;
        let file = std::fs::File::open(&staged)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&staged, &final_path)?;
        if let Ok(dir) = std::fs::File::open(final_path.parent().expect("blob path has a parent")) {
            let _ = dir.sync_all();
        }
        Ok((format_resource_blob_uri(tenant_hash, hash), size, hash))
    }

    /// Single-shot convenience: begin + append + commit in one call.
    pub fn resource_blob_put(
        &self,
        tenant_hash: u64,
        bytes: &[u8],
    ) -> std::io::Result<(String, u64, u64)> {
        let token = self.resource_blob_begin(tenant_hash)?;
        self.resource_blob_append(&token, bytes)?;
        self.resource_blob_commit(tenant_hash, &token)
    }

    /// Range-read a published blob. `length == 0` means "to the end". Returns
    /// (bytes, total_size, eof).
    pub fn resource_blob_fetch(
        &self,
        tenant_hash: u64,
        content_hash: u64,
        offset: u64,
        length: u64,
    ) -> std::io::Result<(Vec<u8>, u64, bool)> {
        let path = self.resource_blob_path(tenant_hash, content_hash);
        let mut file = std::fs::File::open(&path)?;
        let total = file.metadata()?.len();
        if offset >= total {
            return Ok((Vec::new(), total, true));
        }
        let want = if length == 0 {
            total - offset
        } else {
            length.min(total - offset)
        };
        file.seek(std::io::SeekFrom::Start(offset))?;
        let mut bytes = vec![0u8; usize::try_from(want).unwrap_or(usize::MAX)];
        file.read_exact(&mut bytes)?;
        let eof = offset + want >= total;
        Ok((bytes, total, eof))
    }

    /// Delete unreferenced blobs for one tenant, plus stale staging files. `referenced` is the
    /// set of content hashes the caller's manifests still name; only files OLDER than
    /// `min_age_ms` are eligible, so an upload racing the sweep keeps its blob until its
    /// manifest lands.
    pub fn resource_blob_sweep(
        &self,
        tenant_hash: u64,
        referenced: &std::collections::HashSet<u64>,
        min_age_ms: u64,
    ) -> std::io::Result<(u64, u64)> {
        let now = now_ms();
        let old_enough = |path: &std::path::Path| -> bool {
            std::fs::metadata(path)
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|age| age.as_millis() as u64 + min_age_ms <= now)
                .unwrap_or(false)
        };
        let mut scanned = 0u64;
        let mut deleted = 0u64;
        let tenant_dir = self.resource_blob_root().join(format!("{tenant_hash:016x}"));
        if let Ok(entries) = std::fs::read_dir(&tenant_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                let Some(hex) = name.strip_suffix(".bin") else {
                    continue;
                };
                let Ok(content_hash) = u64::from_str_radix(hex, 16) else {
                    continue;
                };
                scanned += 1;
                if !referenced.contains(&content_hash) && old_enough(&path) {
                    if std::fs::remove_file(&path).is_ok() {
                        deleted += 1;
                    }
                }
            }
        }
        let staging_dir = self.resource_blob_root().join(STAGING_DIR);
        if let Ok(entries) = std::fs::read_dir(&staging_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                scanned += 1;
                if old_enough(&path) && std::fs::remove_file(&path).is_ok() {
                    deleted += 1;
                }
            }
        }
        Ok((scanned, deleted))
    }

    /// Dispatch for the blob commands. Runs before the shard lock: blobs live beside the
    /// engine, not inside any shard's record state, and a multi-gigabyte upload must never
    /// hold the shard write lock.
    pub(super) fn execute_resource_blob_command(
        &self,
        request: &ExecuteRequest,
    ) -> Option<ExecuteResponse> {
        let respond = |result: Result<CommandResponse, String>| -> ExecuteResponse {
            match result {
                Ok(response) => ExecuteResponse {
                    status: Status::ok(),
                    response,
                },
                Err(detail) => ExecuteResponse {
                    status: Status::error("resource_blob_error", detail),
                    response: CommandResponse::Empty,
                },
            }
        };
        match &request.command {
            Command::ContextResourceBlobBegin { tenant_hash } => Some(respond(
                self.resource_blob_begin(*tenant_hash)
                    .map(|upload_token| CommandResponse::ContextResourceBlobUpload {
                        upload_token,
                        bytes_total: 0,
                    })
                    .map_err(|err| err.to_string()),
            )),
            Command::ContextResourceBlobAppend {
                upload_token,
                payload_base64,
                ..
            } => Some(respond(
                BASE64
                    .decode(payload_base64)
                    .map_err(|err| format!("invalid base64 payload: {err}"))
                    .and_then(|bytes| {
                        self.resource_blob_append(upload_token, &bytes)
                            .map_err(|err| err.to_string())
                    })
                    .map(|bytes_total| CommandResponse::ContextResourceBlobUpload {
                        upload_token: upload_token.clone(),
                        bytes_total,
                    }),
            )),
            Command::ContextResourceBlobCommit {
                tenant_hash,
                upload_token,
            } => Some(respond(
                self.resource_blob_commit(*tenant_hash, upload_token)
                    .map(
                        |(uri, size_bytes, content_hash)| CommandResponse::ContextResourceBlobCommitted {
                            uri,
                            size_bytes,
                            content_hash,
                        },
                    )
                    .map_err(|err| err.to_string()),
            )),
            Command::ContextResourceBlobPut {
                tenant_hash,
                payload_base64,
            } => Some(respond(
                BASE64
                    .decode(payload_base64)
                    .map_err(|err| format!("invalid base64 payload: {err}"))
                    .and_then(|bytes| {
                        self.resource_blob_put(*tenant_hash, &bytes)
                            .map_err(|err| err.to_string())
                    })
                    .map(
                        |(uri, size_bytes, content_hash)| CommandResponse::ContextResourceBlobCommitted {
                            uri,
                            size_bytes,
                            content_hash,
                        },
                    ),
            )),
            Command::ContextResourceBlobFetch {
                uri,
                offset,
                length,
            } => Some(respond(match parse_resource_blob_uri(uri) {
                None => Err(format!("not a resource blob uri: {uri}")),
                Some((tenant_hash, content_hash)) => self
                    .resource_blob_fetch(tenant_hash, content_hash, *offset, *length)
                    .map(|(bytes, total_size, eof)| CommandResponse::ContextResourceBlobChunk {
                        payload_base64: BASE64.encode(&bytes),
                        total_size,
                        eof,
                    })
                    .map_err(|err| err.to_string()),
            })),
            Command::ContextResourceBlobSweep {
                tenant_hash,
                referenced_content_hashes,
                min_age_ms,
            } => {
                let referenced: std::collections::HashSet<u64> =
                    referenced_content_hashes.iter().copied().collect();
                Some(respond(
                    self.resource_blob_sweep(*tenant_hash, &referenced, *min_age_ms)
                        .map(|(scanned, deleted)| CommandResponse::ContextResourceBlobSwept {
                            scanned,
                            deleted,
                        })
                        .map_err(|err| err.to_string()),
                ))
            }
            _ => None,
        }
    }
}
