use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::control::{
    ScanStreamRequest, ScanStreamResponse, StreamKind, StreamReadRequest, StreamReadResponse,
};
use crate::engine::TemporalEngine;
use crate::http::{post_json_with_options, HttpError, HttpRequestOptions};
use crate::index_log::IndexLogRecord;
use crate::page_store::PageStoreError;
use crate::types::{ExecuteRequest, ShardId, Status};
use crate::wal::WriteAheadLogRecord;

#[derive(Debug, Error)]
pub enum ReplicaReplayError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("http error: {0}")]
    Http(#[from] HttpError),
    #[error("stream {kind:?} failed: {status:?}")]
    StreamFailed { kind: StreamKind, status: Status },
    #[error("page store error: {0}")]
    PageStore(#[from] PageStoreError),
    #[error("oplog replay gap: expected sequence {expected}, got {actual}")]
    OplogGap { expected: u64, actual: u64 },
    #[error("index-log replay gap: expected sequence {expected}, got {actual}")]
    IndexLogGap { expected: u64, actual: u64 },
    #[error("replicated command failed at oplog sequence {sequence}: {status:?}")]
    ApplyFailed { sequence: u64, status: Status },
}

pub trait ReplicaStreamSource {
    fn read_stream(
        &self,
        request: StreamReadRequest,
    ) -> Result<StreamReadResponse, ReplicaReplayError>;

    fn scan_stream(
        &self,
        request: ScanStreamRequest,
    ) -> Result<ScanStreamResponse, ReplicaReplayError>;
}

impl ReplicaStreamSource for TemporalEngine {
    fn read_stream(
        &self,
        request: StreamReadRequest,
    ) -> Result<StreamReadResponse, ReplicaReplayError> {
        Ok(TemporalEngine::read_stream(self, request))
    }

    fn scan_stream(
        &self,
        request: ScanStreamRequest,
    ) -> Result<ScanStreamResponse, ReplicaReplayError> {
        Ok(TemporalEngine::scan_stream(self, request))
    }
}

#[derive(Debug, Clone)]
pub struct HttpReplicaStreamSource {
    addr: String,
    options: HttpRequestOptions,
}

impl HttpReplicaStreamSource {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            options: HttpRequestOptions::default(),
        }
    }

    pub fn with_options(addr: impl Into<String>, options: HttpRequestOptions) -> Self {
        Self {
            addr: addr.into(),
            options,
        }
    }
}

impl ReplicaStreamSource for HttpReplicaStreamSource {
    fn read_stream(
        &self,
        request: StreamReadRequest,
    ) -> Result<StreamReadResponse, ReplicaReplayError> {
        Ok(post_json_with_options(
            &self.addr,
            "/read_stream",
            &request,
            self.options,
        )?)
    }

    fn scan_stream(
        &self,
        request: ScanStreamRequest,
    ) -> Result<ScanStreamResponse, ReplicaReplayError> {
        Ok(post_json_with_options(
            &self.addr,
            "/scan_stream",
            &request,
            self.options,
        )?)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaReplayCursor {
    pub shard_id: ShardId,
    pub checkpoint_installed: bool,
    pub page_segment_ids: Vec<u64>,
    pub index_log_offset: u64,
    pub index_log_sequence: u64,
    pub oplog_offset: u64,
    pub oplog_sequence: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaReplayReport {
    pub shard_id: ShardId,
    pub installed_checkpoint: bool,
    pub installed_page_segments: Vec<u64>,
    pub index_log_records: usize,
    pub oplog_records: usize,
    pub cursor: ReplicaReplayCursor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaReplayRequest {
    pub shard_id: ShardId,
    pub primary_addr: String,
    #[serde(default)]
    pub cursor_path: Option<String>,
    #[serde(default)]
    pub max_stream_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaReplayResponse {
    pub status: Status,
    pub report: Option<ReplicaReplayReport>,
}

#[derive(Debug, Clone)]
pub struct ReplicaReplayOptions {
    pub shard_id: ShardId,
    pub cursor_path: PathBuf,
    pub max_stream_bytes: u64,
}

impl ReplicaReplayOptions {
    pub fn new(shard_id: ShardId, cursor_path: impl Into<PathBuf>) -> Self {
        Self {
            shard_id,
            cursor_path: cursor_path.into(),
            max_stream_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaReplayLoop {
    options: ReplicaReplayOptions,
}

impl ReplicaReplayLoop {
    pub fn new(options: ReplicaReplayOptions) -> Self {
        Self { options }
    }

    pub fn run<S>(
        &self,
        primary: &S,
        follower: &TemporalEngine,
    ) -> Result<ReplicaReplayReport, ReplicaReplayError>
    where
        S: ReplicaStreamSource,
    {
        let mut cursor = self.load_cursor()?;
        cursor.shard_id = self.options.shard_id;
        let mut report = ReplicaReplayReport {
            shard_id: self.options.shard_id,
            cursor: cursor.clone(),
            ..ReplicaReplayReport::default()
        };

        if !cursor.checkpoint_installed {
            self.install_index(primary, follower)?;
            let installed_page_segments = self.install_pages(primary, follower)?;
            follower.load_shard(self.options.shard_id);
            cursor.checkpoint_installed = true;
            cursor.page_segment_ids = installed_page_segments.clone();
            report.installed_checkpoint = true;
            report.installed_page_segments = installed_page_segments;
            self.save_cursor(&mut cursor)?;
        }

        let index = self.replay_index_log(primary, follower, &mut cursor)?;
        if index > 0 {
            follower.load_shard(self.options.shard_id);
            report.index_log_records = index;
            self.save_cursor(&mut cursor)?;
        }

        let oplog = self.replay_oplog(primary, follower, &mut cursor)?;
        if oplog > 0 {
            report.oplog_records = oplog;
            self.save_cursor(&mut cursor)?;
        }

        report.cursor = cursor;
        Ok(report)
    }

    pub fn load_cursor(&self) -> Result<ReplicaReplayCursor, ReplicaReplayError> {
        match fs::read(&self.options.cursor_path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(ReplicaReplayCursor {
                shard_id: self.options.shard_id,
                ..ReplicaReplayCursor::default()
            }),
            Err(err) => Err(err.into()),
        }
    }

    fn save_cursor(&self, cursor: &mut ReplicaReplayCursor) -> Result<(), ReplicaReplayError> {
        cursor.updated_at_ms = now_ms();
        if let Some(parent) = self.options.cursor_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            &self.options.cursor_path,
            serde_json::to_vec_pretty(cursor)?,
        )?;
        Ok(())
    }

    fn install_index<S>(
        &self,
        primary: &S,
        follower: &TemporalEngine,
    ) -> Result<(), ReplicaReplayError>
    where
        S: ReplicaStreamSource,
    {
        let response = primary.read_stream(StreamReadRequest {
            shard_id: self.options.shard_id,
            stream_kind: StreamKind::Index,
            page_segment_id: 0,
            offset: 0,
            size: self.options.max_stream_bytes,
        })?;
        if !response.status.ok {
            return Err(ReplicaReplayError::StreamFailed {
                kind: StreamKind::Index,
                status: response.status,
            });
        }
        follower.install_index_bytes(self.options.shard_id, &response.data)?;
        Ok(())
    }

    fn install_pages<S>(
        &self,
        primary: &S,
        follower: &TemporalEngine,
    ) -> Result<Vec<u64>, ReplicaReplayError>
    where
        S: ReplicaStreamSource,
    {
        follower.load_shard(self.options.shard_id);
        let mut installed = Vec::new();
        for page_segment_id in follower.live_page_segment_ids(self.options.shard_id) {
            let response = primary.read_stream(StreamReadRequest {
                shard_id: self.options.shard_id,
                stream_kind: StreamKind::Page,
                page_segment_id,
                offset: 0,
                size: self.options.max_stream_bytes,
            })?;
            if !response.status.ok {
                return Err(ReplicaReplayError::StreamFailed {
                    kind: StreamKind::Page,
                    status: response.status,
                });
            }
            follower
                .page_store()
                .install_segment(page_segment_id, &response.data)?;
            installed.push(page_segment_id);
        }
        installed.sort_unstable();
        Ok(installed)
    }

    fn replay_index_log<S>(
        &self,
        primary: &S,
        follower: &TemporalEngine,
        cursor: &mut ReplicaReplayCursor,
    ) -> Result<usize, ReplicaReplayError>
    where
        S: ReplicaStreamSource,
    {
        let response = primary.scan_stream(ScanStreamRequest {
            shard_id: self.options.shard_id,
            stream_kind: StreamKind::IndexLog,
            page_segment_id: 0,
            start_offset: cursor.index_log_offset,
            end_offset: u64::MAX,
            max_bytes: self.options.max_stream_bytes,
        })?;
        if !response.status.ok {
            return Err(ReplicaReplayError::StreamFailed {
                kind: StreamKind::IndexLog,
                status: response.status,
            });
        }
        let mut applied = 0;
        let mut expected = cursor.index_log_sequence.saturating_add(1);
        for record in response.records {
            let parsed: IndexLogRecord = serde_json::from_slice(&record.data)?;
            if parsed.sequence != expected {
                return Err(ReplicaReplayError::IndexLogGap {
                    expected,
                    actual: parsed.sequence,
                });
            }
            let index = serde_json::to_vec(&parsed.index)?;
            follower.install_index_bytes(self.options.shard_id, &index)?;
            cursor.index_log_sequence = parsed.sequence;
            cursor.index_log_offset = record.offset.saturating_add(record.data.len() as u64);
            expected = expected.saturating_add(1);
            applied += 1;
        }
        Ok(applied)
    }

    fn replay_oplog<S>(
        &self,
        primary: &S,
        follower: &TemporalEngine,
        cursor: &mut ReplicaReplayCursor,
    ) -> Result<usize, ReplicaReplayError>
    where
        S: ReplicaStreamSource,
    {
        let response = primary.scan_stream(ScanStreamRequest {
            shard_id: self.options.shard_id,
            stream_kind: StreamKind::Wal,
            page_segment_id: 0,
            start_offset: cursor.oplog_offset,
            end_offset: u64::MAX,
            max_bytes: self.options.max_stream_bytes,
        })?;
        if !response.status.ok {
            return Err(ReplicaReplayError::StreamFailed {
                kind: StreamKind::Wal,
                status: response.status,
            });
        }
        let mut applied = 0;
        let mut expected = cursor.oplog_sequence.saturating_add(1);
        for record in response.records {
            let parsed: WriteAheadLogRecord = serde_json::from_slice(&record.data)?;
            if parsed.sequence != expected {
                return Err(ReplicaReplayError::OplogGap {
                    expected,
                    actual: parsed.sequence,
                });
            }
            let response = follower.execute(ExecuteRequest {
                shard_id: self.options.shard_id,
                command: parsed.command,
            });
            if !response.status.ok {
                return Err(ReplicaReplayError::ApplyFailed {
                    sequence: parsed.sequence,
                    status: response.status,
                });
            }
            cursor.oplog_sequence = parsed.sequence;
            cursor.oplog_offset = record.offset.saturating_add(record.data.len() as u64);
            expected = expected.saturating_add(1);
            applied += 1;
        }
        Ok(applied)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::thread;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;

    use super::*;
    use crate::http::{json_response, parse_json, serve};
    use crate::types::{Command, CommandResponse};

    #[test]
    fn replica_replay_installs_pages_index_logs_and_wal_with_cursor_resume() {
        let primary_dir = tempdir().unwrap();
        let follower_dir = tempdir().unwrap();
        let cursor_dir = tempdir().unwrap();
        let primary = TemporalEngine::with_local_dirs(
            1024,
            primary_dir.path().join("cache"),
            primary_dir.path().join("pages"),
            primary_dir.path().join("index"),
        );
        let follower = TemporalEngine::with_local_dirs(
            1024,
            follower_dir.path().join("cache"),
            follower_dir.path().join("pages"),
            follower_dir.path().join("index"),
        );
        primary.load_shard(9);
        primary.execute(ExecuteRequest {
            shard_id: 9,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"v1".to_vec(),
            },
        });
        primary.execute(ExecuteRequest {
            shard_id: 9,
            command: Command::HashSet {
                key: "hash".to_string(),
                field: "f".to_string(),
                value: b"hv".to_vec(),
            },
        });

        let replay = ReplicaReplayLoop::new(ReplicaReplayOptions::new(
            9,
            cursor_dir.path().join("cursor.json"),
        ));
        let first = replay.run(&primary, &follower).unwrap();
        assert!(first.installed_checkpoint);
        assert_eq!(first.index_log_records, 2);
        assert_eq!(first.oplog_records, 2);
        assert_eq!(first.cursor.index_log_sequence, 2);
        assert_eq!(first.cursor.oplog_sequence, 2);

        let read = follower.execute(ExecuteRequest {
            shard_id: 9,
            command: Command::StringGet {
                key: "before".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"v1".to_vec())
            }
        );

        primary.execute(ExecuteRequest {
            shard_id: 9,
            command: Command::StringSet {
                key: "after".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let second = replay.run(&primary, &follower).unwrap();
        assert!(!second.installed_checkpoint);
        assert_eq!(second.index_log_records, 1);
        assert_eq!(second.oplog_records, 1);
        assert_eq!(second.cursor.index_log_sequence, 3);
        assert_eq!(second.cursor.oplog_sequence, 3);

        let third = replay.run(&primary, &follower).unwrap();
        assert_eq!(third.index_log_records, 0);
        assert_eq!(third.oplog_records, 0);
        assert_eq!(third.cursor, second.cursor);

        let read_after = follower.execute(ExecuteRequest {
            shard_id: 9,
            command: Command::StringGet {
                key: "after".to_string(),
            },
        });
        assert_eq!(
            read_after.response,
            CommandResponse::Bytes {
                value: Some(b"v2".to_vec())
            }
        );
    }

    #[test]
    fn replica_replay_can_pull_streams_over_http() {
        let primary_dir = tempdir().unwrap();
        let follower_dir = tempdir().unwrap();
        let cursor_dir = tempdir().unwrap();
        let primary = TemporalEngine::with_local_dirs(
            1024,
            primary_dir.path().join("cache"),
            primary_dir.path().join("pages"),
            primary_dir.path().join("index"),
        );
        let follower = TemporalEngine::with_local_dirs(
            1024,
            follower_dir.path().join("cache"),
            follower_dir.path().join("pages"),
            follower_dir.path().join("index"),
        );
        primary.load_shard(11);
        primary.execute(ExecuteRequest {
            shard_id: 11,
            command: Command::StringSet {
                key: "remote".to_string(),
                value: b"http-stream".to_vec(),
            },
        });

        let addr = free_local_addr();
        let primary_for_server = primary.clone();
        let server_addr = addr.clone();
        thread::spawn(move || {
            serve(&server_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/read_stream") => {
                        match parse_json::<StreamReadRequest>(&request.body) {
                            Ok(req) => json_response(200, &primary_for_server.read_stream(req)),
                            Err(err) => {
                                json_response(400, &Status::error("bad_request", err.to_string()))
                            }
                        }
                    }
                    ("POST", "/scan_stream") => {
                        match parse_json::<ScanStreamRequest>(&request.body) {
                            Ok(req) => json_response(200, &primary_for_server.scan_stream(req)),
                            Err(err) => {
                                json_response(400, &Status::error("bad_request", err.to_string()))
                            }
                        }
                    }
                    ("GET", "/health") => json_response(200, &Status::ok()),
                    _ => json_response(404, &Status::error("not_found", "unknown route")),
                }
            })
            .unwrap()
        });
        wait_for_http(&addr);

        let source = HttpReplicaStreamSource::with_options(
            addr,
            HttpRequestOptions {
                connect_timeout_ms: 1_000,
                io_timeout_ms: 5_000,
                max_retries: 3,
            },
        );
        let replay = ReplicaReplayLoop::new(ReplicaReplayOptions::new(
            11,
            cursor_dir.path().join("cursor.json"),
        ));
        let report = replay.run(&source, &follower).unwrap();
        assert_eq!(report.oplog_records, 1);
        assert_eq!(report.index_log_records, 1);
        assert_eq!(report.installed_page_segments, vec![0]);

        let read = follower.execute(ExecuteRequest {
            shard_id: 11,
            command: Command::StringGet {
                key: "remote".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"http-stream".to_vec())
            }
        );
    }

    fn free_local_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }

    fn wait_for_http(addr: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if crate::http::get_json_with_options::<Status>(
                addr,
                "/health",
                HttpRequestOptions {
                    connect_timeout_ms: 100,
                    io_timeout_ms: 100,
                    max_retries: 0,
                },
            )
            .is_ok()
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "http stream server did not start"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}
