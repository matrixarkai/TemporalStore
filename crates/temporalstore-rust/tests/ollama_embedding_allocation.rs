// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use temporalstore_rust::llm_context::{
    BatchEmbedder, EmbeddingBatch, OllamaConfig, OllamaProvider,
};

thread_local! {
    static TARGET_BYTES: Cell<usize> = const { Cell::new(0) };
    static MATCHING_ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct OutputAllocationCounter;

fn count_request(bytes: usize) {
    let _ = TARGET_BYTES.try_with(|target| {
        if target.get() != 0 && target.get() == bytes {
            let _ = MATCHING_ALLOCATIONS.try_with(|count| count.set(count.get().saturating_add(1)));
        }
    });
}

// The integration-test executable owns this allocator; production binaries do not use it.
unsafe impl GlobalAlloc for OutputAllocationCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: delegate the allocator contract unchanged to System.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            count_request(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: delegate the allocator contract unchanged to System.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            count_request(layout.size());
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: this pointer and layout came from the same System allocator.
        let next = unsafe { System.realloc(pointer, layout, size) };
        if !next.is_null() {
            count_request(size);
        }
        next
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: this pointer and layout came from the same System allocator.
        unsafe { System.dealloc(pointer, layout) };
    }
}

#[global_allocator]
static ALLOCATOR: OutputAllocationCounter = OutputAllocationCounter;

fn count_output_requests<T>(bytes: usize, work: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            TARGET_BYTES.with(|target| target.set(0));
        }
    }
    TARGET_BYTES.with(|target| {
        assert_eq!(target.get(), 0);
        target.set(bytes);
    });
    MATCHING_ALLOCATIONS.with(|count| count.set(0));
    let reset = Reset;
    let output = work();
    let requests = MATCHING_ALLOCATIONS.with(Cell::get);
    drop(reset);
    (output, requests)
}

fn verify_output_allocation(provider: OllamaProvider, dimensions: usize) -> EmbeddingBatch {
    // An odd matrix size stays distinct from power-of-two transport buffer growth.
    let texts = (0..129)
        .map(|index| format!("embedding allocation document {index}"))
        .collect::<Vec<_>>();
    let inputs = texts.iter().map(String::as_str).collect::<Vec<_>>();
    let output_bytes = inputs.len() * dimensions * std::mem::size_of::<f32>();
    let (control, control_requests) = count_output_requests(output_bytes, || {
        black_box(Vec::<f32>::with_capacity(inputs.len() * dimensions))
    });
    assert_eq!(
        control_requests, 1,
        "the counter must recover a planted output-sized allocation"
    );
    drop(control);

    let (output, requests) = count_output_requests(output_bytes, || provider.embed_batch(&inputs));
    let output = output.expect("embedding request");
    assert_eq!(output.dimensions, dimensions);
    assert_eq!(output.values.len(), inputs.len() * dimensions);
    assert!(output.values.iter().all(|value| value.is_finite()));
    println!("rows={} dimensions={dimensions} output_bytes={output_bytes} output_sized_allocations={requests}", inputs.len());
    assert_eq!(
        requests, 1,
        "the first response must not discard a full output-sized reservation"
    );
    output
}

fn fixture_provider(mut config: OllamaConfig) -> (OllamaProvider, Arc<AtomicUsize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let received = Arc::clone(&calls);
    std::thread::spawn(move || {
        temporalstore_rust::http::serve_listener(listener, move |request| {
            assert_eq!(request.path, "/api/embed");
            received.fetch_add(1, Ordering::Relaxed);
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let rows = body["input"].as_array().unwrap();
            let embeddings = rows
                .iter()
                .map(|text| {
                    let ordinal = text
                        .as_str()
                        .unwrap()
                        .rsplit_once(' ')
                        .and_then(|(_, index)| index.parse::<usize>().ok())
                        .unwrap_or(0);
                    vec![ordinal as f32; 384]
                })
                .collect::<Vec<_>>();
            temporalstore_rust::http::json_response(
                200,
                &serde_json::json!({
                    "embeddings": embeddings
                }),
            )
        })
        .unwrap();
    });
    config.base_url = format!("http://{addr}");
    config.timeout_ms = 5_000;
    config.max_retries = 0;
    (OllamaProvider::new(config).unwrap(), calls)
}

#[test]
fn embedding_output_reserves_aggregate_storage_once() {
    let (provider, calls) = fixture_provider(OllamaConfig {
        max_batch_items: 32,
        ..OllamaConfig::default()
    });
    let output = verify_output_allocation(provider, 384);
    for (index, row) in output.values.chunks_exact(384).enumerate() {
        assert!(row.iter().all(|value| *value == index as f32));
    }
    assert_eq!(calls.load(Ordering::Relaxed), 5);
}

#[test]
fn invalid_embedding_tail_is_rejected_before_any_model_call() {
    let (provider, calls) = fixture_provider(OllamaConfig {
        max_batch_items: 1,
        max_input_bytes: 8,
        ..OllamaConfig::default()
    });
    let error = provider
        .embed_batch(&["first", "second", "this input is too large"])
        .unwrap_err();
    assert!(matches!(
        error,
        temporalstore_rust::llm_context::OllamaError::InputTooLarge { index: 2, .. }
    ));
    let calls = calls.load(Ordering::Relaxed);
    println!("invalid_tail_model_calls={calls}");
    assert_eq!(
        calls, 0,
        "invalid batches must not spend model inference on the valid prefix"
    );
}

#[test]
#[ignore = "requires a running model server and an explicit TEMPORALSTORE_OLLAMA_EMBEDDING_MODEL"]
fn embedding_output_reserves_aggregate_storage_once_with_live_model() {
    assert!(
        std::env::var("TEMPORALSTORE_OLLAMA_EMBEDDING_MODEL").is_ok(),
        "select a locally installed model explicitly"
    );
    let mut config = OllamaConfig::from_env();
    config.max_batch_items = 32;
    let provider = OllamaProvider::new(config).unwrap();
    let dimension_probe = provider
        .embed_batch(&["embedding dimension probe"])
        .unwrap();
    verify_output_allocation(provider, dimension_probe.dimensions);
}

#[test]
fn escaped_embedding_tail_is_rejected_before_any_model_call() {
    let mut config = OllamaConfig {
        max_batch_items: 1,
        max_input_bytes: 8,
        ..OllamaConfig::default()
    };
    config.max_request_bytes = serde_json::to_vec(&serde_json::json!({
        "model": config.embedding_model.as_str(),
        "input": ["first"],
        "truncate": config.truncate_embeddings,
        "keep_alive": config.keep_alive.as_str(),
    }))
    .unwrap()
    .len();
    let (provider, calls) = fixture_provider(config);
    let error = provider.embed_batch(&["first", "\"\"\"\""]).unwrap_err();
    assert!(matches!(
        error,
        temporalstore_rust::llm_context::OllamaError::InputTooLarge { index: 1, .. }
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}
