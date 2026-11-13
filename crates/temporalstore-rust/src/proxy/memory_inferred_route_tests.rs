// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn invoke(fixture: &Fixture, method: &str, path: &str, mut body: Value) -> (u16, Value) {
    body["scope"] = json!({"tenant_id":"explicit","account_id":"test"});
    if method == "POST" {
        body["provider"] = fixture.provider.clone();
    }
    let (status, body) = fixture
        .proxy
        .handle_memory_route(&HttpRequest {
            method: method.into(),
            path: path.into(),
            body: serde_json::to_vec(&body).unwrap(),
        })
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn search(fixture: &Fixture, user: Option<&str>) -> Vec<Value> {
    let mut body = json!({"query":"Rust Vim preferences","limit":10});
    if let Some(user) = user {
        body["user_id"] = json!(user);
    }
    let (status, value) = invoke(fixture, "POST", "/memories/search", body);
    assert_eq!(status, 200, "{value}");
    value["results"].as_array().unwrap().clone()
}

#[test]
fn inferred_memory_facts_are_addressable_without_duplicate_inference_or_embeddings() {
    let fixture = fixture(10);
    let raw = "Alice uses Rust and Vim, surrounded by irrelevant conversational filler.";
    let (status, response) = add(&fixture, json!({"messages":raw}));
    assert_eq!(status, 200, "{response}");
    let rows = response["results"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0]["id"], rows[1]["id"]);
    assert_eq!(fixture.chats.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 0);
    let inputs = fixture.inputs.lock().unwrap();
    assert_eq!(
        inputs
            .iter()
            .flatten()
            .filter(|text| text.as_str() == "Rust services")
            .count(),
        1
    );
    assert_eq!(
        inputs
            .iter()
            .flatten()
            .filter(|text| text.as_str() == "Vim editing")
            .count(),
        1
    );
    drop(inputs);
    for row in rows {
        assert_ne!(row["memory"], raw);
        let id = row["id"].as_str().unwrap();
        let (status, got) = invoke(&fixture, "GET", &format!("/memories/{id}"), json!({}));
        assert_eq!(status, 200);
        assert_eq!(got["memory"], row["memory"]);
        let (status, history) = invoke(
            &fixture,
            "GET",
            &format!("/memories/{id}/history"),
            json!({}),
        );
        assert_eq!(status, 200);
        assert_eq!(history["results"][0]["memory"], row["memory"]);
        let record = fixture.proxy.memory_read_one(fixture.tenant, id).unwrap();
        let restored = MemoryRecord::decode(&record.encode()).unwrap();
        assert_eq!(restored.context_ref, record.context_ref);
        assert!(memory_fact_ref(&restored.context_ref).is_some());
    }
    for user in [Some("alice"), None] {
        let found = search(&fixture, user);
        for row in rows {
            assert!(
                found.iter().any(|item| item["id"] == row["id"]),
                "{found:?}"
            );
        }
    }
    let request: MemoryAddRequest = serde_json::from_value(json!({
        "messages":"Bob also has preferences", "user_id":"bob", "provider":fixture.provider,
        "scope":{"tenant_id":"explicit","account_id":"test"}
    }))
    .unwrap();
    let (status, bob) = fixture.proxy.memory_add(request);
    assert_eq!(status, 200);
    let bob: Value = serde_json::from_slice(&bob).unwrap();
    let alice = search(&fixture, Some("alice"));
    assert!(alice.iter().all(|item| !bob["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["id"] == item["id"])));
    assert_eq!(search(&fixture, Some("bob")).len(), 2);
}

#[test]
fn inferred_memory_facts_keep_siblings_after_update_and_delete() {
    let fixture = fixture(10);
    let (status, value) = add(&fixture, json!({"messages":"Use Rust with Vim"}));
    assert_eq!(status, 200, "{value}");
    let rows = value["results"].as_array().unwrap();
    let language = rows
        .iter()
        .find(|row| row["memory"].as_str().unwrap().contains("Rust"))
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let editor = rows
        .iter()
        .find(|row| row["memory"].as_str().unwrap().contains("Vim"))
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let (status, _) = invoke(
        &fixture,
        "PUT",
        &format!("/memories/{language}"),
        json!({"metadata":{"tag":"retained"}}),
    );
    assert_eq!(status, 200);
    assert_eq!(search(&fixture, Some("alice")).len(), 2);
    let (status, removed) = invoke(
        &fixture,
        "DELETE",
        &format!("/memories/{editor}"),
        json!({}),
    );
    assert_eq!(status, 200);
    assert_eq!(removed["deleted"], true);
    let found = search(&fixture, None);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0]["id"], language);
    let (status, _) = invoke(
        &fixture,
        "POST",
        &format!("/memories/{language}/update"),
        json!({"data":"Rust services updated independently"}),
    );
    assert_eq!(status, 200);
    let found = search(&fixture, Some("alice"));
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0]["id"], language);
    assert_eq!(found[0]["memory"], "Rust services updated independently");
    assert_eq!(fixture.chats.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 1);
}

#[test]
fn inferred_memory_rejects_incomplete_or_inconsistent_receipts() {
    for mode in [11, 12, 13, 14, 18] {
        let fixture = fixture(mode);
        let (status, value) = add(
            &fixture,
            json!({"messages":["First source", "Second source"]}),
        );
        assert_eq!(status, 502, "mode={mode} {value}");
        assert_eq!(value["code"], "invalid_memory_extraction_response");
        assert!(fixture
            .proxy
            .memory_read_all(fixture.tenant)
            .unwrap()
            .is_empty());
    }
}

#[test]
fn inferred_memory_without_facts_does_not_catalog_raw_messages() {
    let fixture = fixture(15);
    let (status, value) = add(&fixture, json!({"messages":"No durable fact here"}));
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["results"], json!([]));
    assert!(fixture
        .proxy
        .memory_read_all(fixture.tenant)
        .unwrap()
        .is_empty());
    assert_eq!(fixture.chats.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 0);
}

#[test]
fn inferred_memory_requires_the_fact_endpoint_without_raw_fallback() {
    let fixture = fixture(19);
    let (status, _) = add(&fixture, json!({"messages":"A source requiring inference"}));
    assert_ne!(status, 200);
    assert_eq!(fixture.inferred.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.chats.load(Ordering::Relaxed), 0);
    assert!(fixture
        .proxy
        .memory_read_all(fixture.tenant)
        .unwrap()
        .is_empty());
}

#[test]
fn inferred_memory_bounds_metadata_fanout_before_backend_work() {
    for infer in [false, true] {
        let fixture = fixture(10);
        let messages = vec!["Source"; MEMORY_BATCH_LIMIT];
        let (status, value) = add(
            &fixture,
            json!({"infer":infer,"messages":messages,
            "metadata":{"blob":"x".repeat(24 * 1024)}}),
        );
        assert_eq!(status, 400, "{value}");
        assert_eq!(value["code"], "bad_request");
        assert_eq!(fixture.inferred.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.explicit.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.chats.load(Ordering::Relaxed), 0);
        assert!(fixture
            .proxy
            .memory_read_all(fixture.tenant)
            .unwrap()
            .is_empty());
    }
}

#[test]
fn inferred_memory_fact_pages_preserve_all_results_and_counts() {
    let fixture = fixture(10);
    let messages = (0..65)
        .map(|index| format!("Source {index} has preferences"))
        .collect::<Vec<_>>();
    let (status, value) = add(&fixture, json!({"messages":messages}));
    assert_eq!(status, 200, "{value}");
    let rows = value["results"].as_array().unwrap();
    assert_eq!(rows.len(), 130);
    let ids = rows
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect::<HashSet<_>>();
    assert_eq!(ids.len(), 130);
    assert_eq!(fixture.chats.load(Ordering::Relaxed), 65);
    assert_eq!(fixture.explicit.load(Ordering::Relaxed), 0);
    let total = fixture.proxy.execute(ExecuteRequest {
        shard_id: fixture.proxy.context_shard_id(fixture.tenant),
        command: Command::ZSetScore {
            key: memory_identity_key(fixture.tenant),
            member: MEMORY_TOTAL_MEMBER.to_vec(),
        },
    });
    assert!(total.status.ok);
    assert_eq!(
        total.response,
        CommandResponse::Bytes {
            value: Some(b"130".to_vec())
        }
    );
    assert_eq!(
        fixture.proxy.memory_read_all(fixture.tenant).unwrap().len(),
        130
    );
}

#[test]
fn inferred_memory_duplicate_entity_identity_keeps_last_persisted_value() {
    let fixture = fixture(17);
    let (status, value) = add(&fixture, json!({"messages":"A preference was corrected"}));
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["results"].as_array().unwrap().len(), 1);
    assert_eq!(value["results"][0]["memory"], "language: Go services");
    let found = search(&fixture, None);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0]["memory"], "language: Go services");
}
