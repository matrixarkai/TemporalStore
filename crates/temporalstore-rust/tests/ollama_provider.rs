// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::thread;

use temporalstore_rust::llm_context::{
    BatchEmbedder, BatchExtractor, ContextMessage, ContextRole, OllamaConfig, OllamaError,
    OllamaProvider,
};

fn provider(addr: SocketAddr) -> OllamaProvider {
    OllamaProvider::new(OllamaConfig {
        base_url: format!("http://{addr}"),
        max_batch_items: 2,
        ..OllamaConfig::default()
    })
    .unwrap()
}

fn serve_json(responses: Vec<&'static str>) -> (SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let count = stream.read(&mut buf).unwrap();
                request.extend_from_slice(&buf[..count]);
                let Some(header_end) = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|offset| offset + 4)
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("Content-Length: ")
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if request.len() >= header_end + content_length {
                    break;
                }
            }
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response.len(),
                response
            );
            stream.write_all(reply.as_bytes()).unwrap();
        }
    });
    (addr, handle)
}

#[test]
fn ollama_native_embedding_batches_and_flattens_rows() {
    let (addr, server) = serve_json(vec![
        r#"{"embeddings":[[1.0,0.0],[0.0,1.0]]}"#,
        r#"{"embeddings":[[0.5,0.5]]}"#,
    ]);
    let output = provider(addr)
        .embed_batch(&["one", "two", "three"])
        .unwrap();
    server.join().unwrap();

    assert_eq!(output.dimensions, 2);
    assert_eq!(output.values, vec![1.0, 0.0, 0.0, 1.0, 0.5, 0.5]);
}

#[test]
fn ollama_native_extraction_maps_source_metadata() {
    let (addr, server) = serve_json(vec![
        r#"{"message":{"content":"{\"items\":[{\"source_message_id\":7,\"text\":\"Prefers local models\"}]}"}}"#,
    ]);
    let output = provider(addr)
        .extract_batch(&[ContextMessage {
            tenant_id: 4,
            message_id: 7,
            event_time_ms: 11,
            role: ContextRole::User,
            text: "I prefer local models".to_string(),
        }])
        .unwrap();
    server.join().unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].tenant_id, 4);
    assert_eq!(output[0].source_message_id, 7);
    assert_eq!(output[0].event_time_ms, 11);
    assert_eq!(output[0].text, "Prefers local models");
}

#[test]
fn ollama_rejects_invalid_or_oversized_inputs_before_io() {
    let provider = OllamaProvider::new(OllamaConfig {
        max_input_bytes: 3,
        ..OllamaConfig::default()
    })
    .unwrap();
    assert!(matches!(
        provider.embed_batch(&["four"]),
        Err(OllamaError::InputTooLarge { .. })
    ));
    assert!(matches!(
        OllamaProvider::new(OllamaConfig {
            base_url: "https://localhost:11434".to_string(),
            ..OllamaConfig::default()
        }),
        Err(OllamaError::UnsupportedUrl)
    ));
}
