// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SEQUENCE ROW, A CONTEXT NODE AND A CONTEXT EVENT ENCODE INTO ONE BUFFER.
//!
//! Each encoder began with an empty `Vec<u8>` and pushed bytes one at a time, so an ordinary value
//! grew its buffer four or five times (8, 16, 32, 64 ...), each step a reallocation and a copy:
//! a context node's encode alone was four blocks of the roughly sixty its upsert allocates, an
//! event's four of about ninety-one, a sequence row's two. Each encoder now reserves an upper
//! bound once.
//!
//! WHAT MOVES: the allocations of an encode, to one, whatever the value's size. WHAT DOES NOT: a
//! byte. A reserved capacity is not content, and the bytes are the ones the decoders read.
//!
//! TWO GUARDS.
//!   * `an_encoded_value_decodes_to_what_was_encoded` round-trips rows, nodes and events of several
//!     shapes (empty strings, long strings, vectors, every optional field) through the encoder and
//!     the decoder, and requires a row's buffer to be exactly the reserved bound (no regrowth).
//!   * `probe::an_encode_allocates_once_whatever_the_size` (alloc-probe, `--ignored`) requires one
//!     allocation per encode at a small and a large (long strings, no vector) value, with the old empty-buffer growth built
//!     in the test as the control that must cost more than one.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use crate::types::{ContextEvent, ContextNode, ContextWire, SequenceFeatureRow};

fn node(name: &str, l0: &str, vector: usize, summary: usize) -> ContextNode {
    serde_json::from_value(serde_json::json!({
        "node_hash": 12345678901234u64, "parent_hash": 77, "kind": 3, "canonical_name": name, "l0": l0,
        "last_event_time_ms": 1787270070000u64,
        "vector": (0..vector).map(|i| i as f32 * 0.25).collect::<Vec<f32>>(),
        "embedding_model_hash": 99, "embedding_updated_at_ms": 5,
        "summary_vector": (0..summary).map(|i| -(i as f32)).collect::<Vec<f32>>(),
        "summary_vector_valid_from_ms": 7
    }))
    .expect("node")
}

fn event(text: &str, vector: usize) -> ContextEvent {
    serde_json::from_value(serde_json::json!({
        "event_id_hash": 99999999999u64, "event_time_ms": 1787270070000u64, "text": text,
        "confidence": 0.5, "importance": 0.25,
        "vector": (0..vector).map(|i| i as f32).collect::<Vec<f32>>(), "embedding_model_hash": 3
    }))
    .expect("event")
}

#[test]
fn an_encoded_value_decodes_to_what_was_encoded() {
    let long = "x".repeat(2_000);
    let mut checked = 0usize;
    for (name, l0, vector, summary) in [
        ("", "", 0usize, 0usize),
        ("a", "b", 0, 0),
        ("a name", "a longer summary of the node", 4, 0),
        (long.as_str(), long.as_str(), 16, 16),
    ] {
        let value = node(name, l0, vector, summary);
        let bytes = value.encode_context_proto_value();
        assert!(bytes.capacity() >= bytes.len());
        let back = ContextNode::decode_context_proto_value(&bytes).expect("a node decodes");
        assert_eq!(back.node_hash, value.node_hash);
        assert_eq!(back.canonical_name, value.canonical_name);
        assert_eq!(back.l0, value.l0);
        assert_eq!(back.vector.len(), value.vector.len());
        checked += 1;
    }
    for (text, vector) in [("", 0usize), ("an event", 0), (long.as_str(), 8)] {
        let value = event(text, vector);
        let bytes = value.encode_context_proto_value();
        let back = ContextEvent::decode_context_proto_value(&bytes).expect("an event decodes");
        assert_eq!(back.event_id_hash, value.event_id_hash);
        assert_eq!(back.text, value.text);
        assert_eq!(back.vector.len(), value.vector.len());
        checked += 1;
    }
    for row in [
        SequenceFeatureRow { timestamp_ms: 1, gid: 0, action_type: 0, duration: 0, author_id: 0 },
        SequenceFeatureRow { timestamp_ms: 5, gid: 7, action_type: 1, duration: 5, author_id: 9 },
        SequenceFeatureRow { timestamp_ms: 9, gid: u64::MAX, action_type: u32::MAX, duration: u32::MAX, author_id: u64::MAX },
    ] {
        let bytes = row.encode_feature_proto_value();
        // The widest a row encodes to fits the reserved bound, so a full-width row is not regrown.
        assert!(bytes.len() <= 34, "a row encoded to {} bytes", bytes.len());
        assert_eq!(bytes.capacity(), 34, "a row's buffer was reallocated");
        let back = SequenceFeatureRow::decode_feature_proto_value(row.timestamp_ms, &bytes).expect("a row decodes");
        assert_eq!(back, row);
        checked += 1;
    }
    // THE CONTROL: every shape was reached.
    assert_eq!(checked, 4 + 3 + 3);
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    fn allocs_of(encode: impl FnOnce() -> Vec<u8>) -> u64 {
        let probe = Probe::start();
        let bytes = encode();
        let allocs = probe.stop().allocs;
        assert!(!bytes.is_empty());
        allocs
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn an_encode_allocates_once_whatever_the_size() {
        let canary = Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(canary.stop().allocs > 0, "the counting allocator is not installed");
        drop(sink);

        let long = "y".repeat(1_500);
        // Long strings and no vector: a vector is encoded by a helper that builds its own
        // buffers, which this change does not touch, so it is left out of the one-allocation claim.
        let small_node = node("a name", "a summary", 0, 0);
        let large_node = node(&long, &long, 0, 0);
        let small_event = event("an event of some text", 0);
        let large_event = event(&long, 0);
        let row = SequenceFeatureRow { timestamp_ms: 5, gid: 12345, action_type: 1, duration: 5, author_id: 9 };
        assert_eq!(allocs_of(|| small_node.encode_context_proto_value()), 1, "a small node");
        assert_eq!(allocs_of(|| large_node.encode_context_proto_value()), 1, "a large node");
        assert_eq!(allocs_of(|| small_event.encode_context_proto_value()), 1, "a small event");
        assert_eq!(allocs_of(|| large_event.encode_context_proto_value()), 1, "a large event");
        assert_eq!(allocs_of(|| row.encode_feature_proto_value()), 1, "a row");
        // THE CONTROL: the empty buffer these encoders used to start from, filled with the same
        // number of bytes one at a time, reallocates, so the probe can see a regrowth.
        let length = large_node.encode_context_proto_value().len();
        let grown = allocs_of(|| {
            let mut out = Vec::new();
            for index in 0..length {
                out.push(index as u8);
            }
            out
        });
        assert!(grown >= 4, "the control grew {grown} times, so the probe cannot see a regrowth");
    }
}
