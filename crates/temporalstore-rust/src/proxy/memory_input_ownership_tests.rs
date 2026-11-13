// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn memory_input_plain_text_moves_owned_buffer() {
    for infer in [false, true] {
        for many in [false, true] {
            let text = "Rust services ".repeat(4096);
            let pointer = text.as_ptr();
            let capacity = text.capacity();
            let messages = if many {
                MemoryMessages::Many(vec![Value::String(text)])
            } else {
                MemoryMessages::Text(text)
            };
            let rows = normalized_messages(messages, infer).unwrap();
            assert_eq!(rows[0].1.as_ptr(), pointer);
            assert_eq!(rows[0].1.capacity(), capacity);
            assert_eq!(rows[0].1.ends_with(' '), !infer);
        }
    }
}

#[test]
fn memory_input_object_fields_move_owned_buffers() {
    for infer in [false, true] {
        let value =
            json!({"role":"assistant", "content":"Rust services".repeat(4096), "name":"worker"});
        let role = value["role"].as_str().unwrap().as_ptr();
        let text = value["content"].as_str().unwrap().as_ptr();
        let name = value["name"].as_str().unwrap().as_ptr();
        let rows = normalized_messages(MemoryMessages::Many(vec![value]), infer).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.as_ptr(), role);
        assert_eq!(rows[0].1.as_ptr(), text);
        assert_eq!(rows[0].2.as_ref().unwrap().as_ptr(), name);
    }
}

#[test]
fn memory_input_unicode_trimming_preserves_the_allocation() {
    for infer in [false, true] {
        let text = String::from("\u{2003}\tRust \u{1f4bb} services\n\u{3000}");
        let pointer = text.as_ptr();
        let capacity = text.capacity();
        let rows = normalized_messages(MemoryMessages::Text(text), infer).unwrap();
        assert_eq!(rows[0].1.as_ptr(), pointer);
        assert_eq!(rows[0].1.capacity(), capacity);
        assert_eq!(
            rows[0].1,
            if infer {
                "Rust \u{1f4bb} services"
            } else {
                "\u{2003}\tRust \u{1f4bb} services\n\u{3000}"
            }
        );
    }
}

#[test]
fn memory_input_alias_and_role_filtering_preserve_the_contract() {
    let messages = || {
        MemoryMessages::Many(vec![
            json!({"role":"system","content":"system constraint"}),
            json!({"content":"implicit user"}),
            json!({"role":"user","text":" alias ","name":""}),
            json!({"role":"assistant","content":null,"text":"not a fallback"}),
            json!({"role":"assistant","content":"priority","text":"ignored","name":12}),
        ])
    };
    let explicit = normalized_messages(messages(), false).unwrap();
    assert_eq!(
        explicit,
        vec![
            ("user".into(), " alias ".into(), None),
            ("assistant".into(), "priority".into(), None),
        ]
    );
    let inferred = normalized_messages(messages(), true).unwrap();
    assert_eq!(
        inferred,
        vec![
            ("system".into(), "system constraint".into(), None),
            ("user".into(), "implicit user".into(), None),
            ("user".into(), "alias".into(), None),
            ("assistant".into(), "priority".into(), None),
        ]
    );
}

#[test]
fn memory_input_empty_and_non_string_values_preserve_the_contract() {
    for infer in [false, true] {
        assert!(normalized_messages(MemoryMessages::Text(" \t\u{2003}".into()), infer).is_err());
        let values = MemoryMessages::Many(vec![
            Value::Null,
            json!(17),
            json!(" \t"),
            json!({"role":"user","content":false}),
        ]);
        let result = normalized_messages(values, infer);
        if infer {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_empty());
        }
    }
}

fn input_record(index: u64, body: String) -> MemoryRecord {
    let id = memory_id(42, 100, index, &body);
    MemoryRecord {
        context_ref: id.clone(),
        id,
        memory: body,
        metadata: json!({"topic":"Rust"}),
        tenant_id: "tenant".into(),
        account_id: "account".into(),
        user_id: "alice".into(),
        agent_id: "agent".into(),
        run_id: "session".into(),
        created_at_ms: 100,
        updated_at_ms: 100 + index,
    }
}

#[test]
fn memory_input_extraction_moves_bodies_and_preserves_identity() {
    let mut records = (0..8)
        .map(|index| input_record(index, "x".repeat(32 * 1024)))
        .collect::<Vec<_>>();
    let pointers = records
        .iter()
        .map(|row| row.memory.as_ptr())
        .collect::<Vec<_>>();
    let ids = records.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
    let nodes = records
        .iter()
        .map(|row| memory_record_node_hash(42, row))
        .collect::<Vec<_>>();
    let request = memory_extract_request(
        1,
        42,
        &mut records,
        vec!["user".into(); 8],
        Default::default(),
    );
    assert_eq!(request.max_events, 8);
    assert_eq!(request.end_time_ms, 108);
    for (index, source) in request.sources.iter().enumerate() {
        assert_eq!(source.body.as_ptr(), pointers[index]);
        assert_eq!(source.body.len(), 32 * 1024);
        assert_eq!(source.source_id, ids[index]);
        assert_eq!(source.title, "user");
        assert_eq!(records[index].memory.capacity(), 0);
        assert!(records[index].memory.is_empty());
        assert_eq!(records[index].metadata, json!({"topic":"Rust"}));
        assert_eq!(records[index].user_id, "alice");
        assert_eq!(memory_record_node_hash(42, &records[index]), nodes[index]);
    }
    let bytes = crate::context_workflow::ingest_wire::encode_request(request).unwrap();
    let decoded = crate::context_workflow::ingest_wire::decode_request(&bytes).unwrap();
    assert_eq!(decoded.sources.len(), 8);
    assert!(decoded
        .sources
        .iter()
        .all(|source| source.body == "x".repeat(32 * 1024)));
}

#[test]
fn memory_input_fact_projection_does_not_need_raw_source_text() {
    use crate::sdk::v1 as wire;
    use prost::Message;
    let mut records = vec![input_record(
        0,
        "Remember my preferred editor is Vim.".into(),
    )];
    let original = records[0].id.clone();
    let node = memory_record_node_hash(42, &records[0]);
    let request =
        memory_extract_request(1, 42, &mut records, vec!["user".into()], Default::default());
    assert_eq!(
        request.sources[0].body,
        "Remember my preferred editor is Vim."
    );
    assert!(records[0].memory.is_empty());
    let entity = crate::context_workflow::ContextExtractedEntity {
        kind: "preference".into(),
        name: "editor".into(),
        value: "Vim".into(),
    };
    let hash = crate::context_workflow::context_extracted_entity_hash(&entity);
    let bytes = wire::ContextMemoryIngestResponse {
        acknowledgment: Some(wire::ContextExplicitIngestResponse {
            protocol_version: crate::context_workflow::ingest_wire::EXPLICIT_PROTOCOL_VERSION,
            status: Some(wire::Status {
                ok: true,
                code: String::new(),
                message: String::new(),
            }),
            accepted: 1,
            failed: 0,
        }),
        sources: vec![wire::ContextMemorySourceReceipt {
            status: Some(wire::Status {
                ok: true,
                code: String::new(),
                message: String::new(),
            }),
            source_ref: original.clone(),
            node_hash: node,
            extracted_entity_count: 1,
            facts: vec![wire::ContextMemoryFact {
                entity_hash: hash,
                kind: entity.kind,
                name: entity.name,
                value: entity.value,
            }],
        }],
    }
    .encode_to_vec();
    let facts = memory_inferred_records(42, records, &bytes).unwrap();
    assert_eq!(facts[0].id, memory_id(42, 100, hash, &original));
    assert_eq!(facts[0].memory, "editor: Vim");
    assert_eq!(facts[0].metadata, json!({"topic":"Rust"}));
    assert_eq!(facts[0].run_id, "session");
    assert_eq!(memory_record_node_hash(42, &facts[0]), node);
}
