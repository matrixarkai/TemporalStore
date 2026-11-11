// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use temporalstore_rust::context_workflow::ingest_wire;
use temporalstore_rust::http::{RequestHead, StreamAction, StreamTransfer};
use temporalstore_rust::{Status, TemporalEngine};

pub fn handle(
    engine: &TemporalEngine,
    head: &RequestHead,
    transfer: &mut StreamTransfer,
) -> StreamAction {
    if head.method != "POST" || head.path != "/context/ingest_explicit" {
        return StreamAction::Declined;
    }
    let result = if transfer.body_len() > ingest_wire::EXPLICIT_MAX_BODY_BYTES {
        Err(Status::error(
            "context_explicit_body_too_large",
            "explicit context body exceeds limit",
        ))
    } else {
        let mut body = Vec::with_capacity(transfer.body_len());
        let mut scratch = [0u8; 8192];
        let read = (|| -> std::io::Result<()> {
            loop {
                let remaining = transfer.body_len().saturating_sub(body.len());
                if remaining == 0 {
                    break;
                }
                let take = remaining.min(scratch.len());
                let read = transfer.read_body(&mut scratch[..take])?;
                if read == 0 {
                    break;
                }
                body.extend_from_slice(&scratch[..read]);
            }
            Ok(())
        })();
        match read {
            Ok(()) if body.len() == transfer.body_len() => ingest_wire::decode_request(&body),
            Ok(()) => Err(Status::error(
                "context_explicit_body_incomplete",
                "incomplete explicit context body",
            )),
            Err(error) => Err(Status::error(
                "context_explicit_body_read",
                error.to_string(),
            )),
        }
    };
    let (http_status, body) = match result {
        Ok(request) => (200, ingest_wire::execute_request(engine, request)),
        Err(status) => (400, ingest_wire::encode_response(status, 0, 0)),
    };
    let _ = transfer.drain_body();
    if transfer
        .send_head(http_status, "application/x-protobuf", body.len())
        .is_ok()
    {
        let _ = transfer.write_chunk(&body);
        let _ = transfer.flush();
    }
    StreamAction::Handled
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use temporalstore_rust::context_workflow::{
        ContextIngestExtractRequest, ContextModelProviderConfig,
    };
    use temporalstore_rust::http::serve_listener_with_stream_handler;

    #[test]
    fn explicit_route_sends_protobuf_headers_and_rejects_json() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        std::thread::spawn(move || {
            serve_listener_with_stream_handler(
                listener,
                move |head, transfer| handle(&engine, head, transfer),
                |_| panic!("explicit route was not handled"),
            )
            .unwrap();
        });
        let valid = ingest_wire::encode_request(ContextIngestExtractRequest {
            shard_id: 1,
            tenant_hash: 42,
            sources: Vec::new(),
            query: String::new(),
            start_time_ms: 0,
            end_time_ms: 1,
            max_events: 1,
            provider: ContextModelProviderConfig::default(),
        })
        .unwrap();
        for (body, expected_status) in [(valid, 200), (b"{}".to_vec(), 400)] {
            let mut socket = TcpStream::connect(addr).unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            write!(socket, "POST /context/ingest_explicit HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
            socket.write_all(&body).unwrap();
            let mut response = Vec::new();
            socket.read_to_end(&mut response).unwrap();
            let end = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .unwrap()
                + 4;
            let header = std::str::from_utf8(&response[..end]).unwrap();
            assert!(header.starts_with(&format!("HTTP/1.1 {expected_status} ")));
            assert!(header.contains("Content-Type: application/x-protobuf\r\n"));
            assert_eq!(
                ingest_wire::validate_response(&response[end..], 0).is_ok(),
                expected_status == 200
            );
        }
    }
}
