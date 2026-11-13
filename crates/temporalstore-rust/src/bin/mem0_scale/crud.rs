// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

const APIS: &[&str] = &[
    "crud_add",
    "crud_update",
    "crud_get",
    "crud_history",
    "crud_feedback",
    "crud_list",
    "crud_users",
    "crud_batch_update",
    "crud_batch_delete",
    "crud_delete",
    "crud_delete_all",
    "crud_reset",
    "crud_batch_verify",
    "crud_delete_all_verify",
    "crud_reset_verify",
];

fn measured<T>(
    timings: &mut Timings,
    failures: &mut Vec<String>,
    name: &str,
    call: impl FnOnce() -> Result<Value, String>,
    check: impl FnOnce(Value) -> Result<T, String>,
) -> Option<T> {
    let started = Instant::now();
    let result = call().and_then(check);
    timings.observe(name, started, &result);
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            failures.push(format!("{name}: {error}"));
            None
        }
    }
}

fn count(response: &Value, field: &str, expected: usize) -> Result<(), String> {
    if response[field].as_u64() == Some(expected as u64) {
        Ok(())
    } else {
        Err(format!(
            "{field} acknowledgment count differs from {expected}"
        ))
    }
}

fn added_ids(response: Value, expected: usize) -> Result<Vec<String>, String> {
    let rows = response["results"]
        .as_array()
        .ok_or("missing add results")?;
    let ids = rows
        .iter()
        .map(|row| {
            row["id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| "missing add id".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if ids.len() != expected || ids.iter().collect::<HashSet<_>>().len() != expected {
        return Err("add returned incomplete or duplicate ids".into());
    }
    Ok(ids)
}

fn body(options: &Options, tenant: &str, user: &str, round: usize, records: usize) -> Value {
    let mut value = scope_body(tenant);
    value["user_id"] = json!(user);
    value["infer"] = json!(options.infer);
    value["messages"] = json!((0..records)
        .map(|item| json!({
            "role":"user", "content":corpus_text(round * 32 + item, options.payload_bytes)
        }))
        .collect::<Vec<_>>());
    with_provider(options, &mut value);
    value
}

fn with_provider(options: &Options, body: &mut Value) {
    if let Some(provider) = &options.provider {
        body["provider"] = provider.clone();
    }
}

pub(super) fn run(
    options: &Options,
    tenant: &str,
    timings: &mut Timings,
    failures: &mut Vec<String>,
) {
    for round in 0..options.crud_rounds {
        let tenant = corpus_tenant(tenant, round % options.tenant_shards);
        let user = format!("crud-user-{round:08}");
        let seed = body(options, &tenant, &user, round, 20);
        let Some(ids) = measured(
            timings,
            failures,
            "crud_add",
            || post(&options.addr, "/memories", &seed),
            |value| added_ids(value, 20),
        ) else {
            continue;
        };
        let id = &ids[0];
        let text = format!("updated CRUD decision {round}");
        let mut update = json!({"data":text});
        with_provider(options, &mut update);
        measured(
            timings,
            failures,
            "crud_update",
            || post(&options.addr, &format!("/memories/{id}/update"), &update),
            |value| {
                if value["updated"] == true {
                    Ok(())
                } else {
                    Err("missing update acknowledgment".into())
                }
            },
        );
        measured(
            timings,
            failures,
            "crud_get",
            || {
                get_json_with_options::<Value>(
                    &options.addr,
                    &format!("/memories/{id}"),
                    request_options(),
                )
                .map_err(|error| error.to_string())
                .and_then(validate_response)
            },
            |value| {
                if value["id"] == *id && value["memory"] == text {
                    Ok(())
                } else {
                    Err("updated memory is not readable".into())
                }
            },
        );
        measured(
            timings,
            failures,
            "crud_history",
            || {
                get_json_with_options::<Value>(
                    &options.addr,
                    &format!("/memories/{id}/history"),
                    request_options(),
                )
                .map_err(|error| error.to_string())
                .and_then(validate_response)
            },
            |value| {
                if value["results"].as_array().is_some_and(|rows| {
                    rows.iter()
                        .any(|row| row["id"] == *id && row["event"] == "UPDATE")
                }) {
                    Ok(())
                } else {
                    Err("update history is missing".into())
                }
            },
        );
        measured(
            timings,
            failures,
            "crud_feedback",
            || {
                post(
                    &options.addr,
                    &format!("/memories/{id}/feedback"),
                    &json!({"feedback":"POSITIVE"}),
                )
            },
            |value| {
                if value["recorded"] == true {
                    Ok(())
                } else {
                    Err("feedback acknowledgment missing".into())
                }
            },
        );

        let mut query = scope_body(&tenant);
        query["user_id"] = json!(user);
        query["limit"] = json!(20);
        measured(
            timings,
            failures,
            "crud_list",
            || post(&options.addr, "/memories/list", &query),
            |value| {
                let returned = added_ids(value, 20)?;
                if returned.iter().collect::<HashSet<_>>() == ids.iter().collect::<HashSet<_>>() {
                    Ok(())
                } else {
                    Err("list returned incorrect scoped memories".into())
                }
            },
        );
        measured(
            timings,
            failures,
            "crud_users",
            || post(&options.addr, "/memories/users", &query),
            |value| {
                if value["results"].as_array().is_some_and(|rows| {
                    !rows.is_empty()
                        && rows.len() <= 20
                        && rows.iter().all(|row| {
                            matches!(row["type"].as_str(), Some("user" | "agent" | "run"))
                                && row["name"].as_str().is_some_and(|name| !name.is_empty())
                        })
                }) {
                    Ok(())
                } else {
                    Err("users response missing results".into())
                }
            },
        );

        let batch_ids = &ids[1..17];
        let batch_text = format!("batch updated CRUD decision {round}");
        let mut batch = json!({"memories":batch_ids.iter().map(|id| json!({
            "memory_id":id,"text":batch_text
        })).collect::<Vec<_>>()});
        with_provider(options, &mut batch);
        measured(
            timings,
            failures,
            "crud_batch_update",
            || post(&options.addr, "/memories/batch/update", &batch),
            |value| count(&value, "updated", 16),
        );
        measured(
            timings,
            failures,
            "crud_batch_verify",
            || {
                get_json_with_options::<Value>(
                    &options.addr,
                    &format!("/memories/{}", batch_ids[0]),
                    request_options(),
                )
                .map_err(|error| error.to_string())
                .and_then(validate_response)
            },
            |value| {
                if value["memory"] == batch_text {
                    Ok(())
                } else {
                    Err("batch update is not readable".into())
                }
            },
        );
        measured(
            timings,
            failures,
            "crud_batch_delete",
            || {
                post(
                    &options.addr,
                    "/memories/batch/delete",
                    &json!({"memories":batch_ids}),
                )
            },
            |value| count(&value, "deleted", 16),
        );
        measured(
            timings,
            failures,
            "crud_delete",
            || post(&options.addr, &format!("/memories/{id}/delete"), &json!({})),
            |value| {
                if value["deleted"] == true {
                    Ok(())
                } else {
                    Err("delete acknowledgment missing".into())
                }
            },
        );
        // Three seeded rows remain after the single and sixteen-item deletes.
        measured(
            timings,
            failures,
            "crud_delete_all",
            || post(&options.addr, "/memories/delete_all", &query),
            |value| count(&value, "deleted", 3),
        );
        measured(
            timings,
            failures,
            "crud_delete_all_verify",
            || post(&options.addr, "/memories/list", &query),
            empty_results,
        );

        let reset_tenant = format!("{tenant}-crud-reset");
        let reset_seed = body(options, &reset_tenant, "reset-user", round, 2);
        let seeded =
            post(&options.addr, "/memories", &reset_seed).and_then(|value| added_ids(value, 2));
        if seeded.is_err() {
            failures.push("crud reset fixture could not be seeded".into());
        }
        measured(
            timings,
            failures,
            "crud_reset",
            || post(&options.addr, "/memories/reset", &scope_body(&reset_tenant)),
            |value| count(&value, "deleted", 2),
        );
        measured(
            timings,
            failures,
            "crud_reset_verify",
            || post(&options.addr, "/memories/list", &scope_body(&reset_tenant)),
            empty_results,
        );
    }
    check_coverage(timings, options.crud_rounds, failures);
}

fn check_coverage(timings: &Timings, rounds: usize, failures: &mut Vec<String>) {
    for name in APIS {
        let actual = timings.values.get(*name).map_or(0, Vec::len);
        if actual != rounds {
            failures.push(format!(
                "{name}: incomplete coverage ({actual}/{rounds} rounds)"
            ));
        }
    }
}

fn empty_results(value: Value) -> Result<(), String> {
    if value["results"].as_array().is_some_and(Vec::is_empty) {
        Ok(())
    } else {
        Err("deleted memories remain listed or results are missing".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_samples_cannot_pass_as_complete_coverage() {
        let mut timings = Timings::default();
        let mut failures = Vec::new();
        check_coverage(&timings, 0, &mut failures);
        assert!(failures.is_empty());
        for name in APIS {
            timings.values.insert((*name).into(), vec![10, 20]);
        }
        timings.values.get_mut("crud_reset").unwrap().pop();
        check_coverage(&timings, 2, &mut failures);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("crud_reset: incomplete coverage (1/2 rounds)"));
    }

    #[test]
    fn incomplete_duplicate_and_missing_ids_are_rejected() {
        for value in [
            json!({}),
            json!({"results":[]}),
            json!({"results":[{"id":""}]}),
            json!({"results":[{"id":"a"},{"id":"a"}]}),
        ] {
            assert!(added_ids(value, 2).is_err());
        }
        assert_eq!(
            added_ids(json!({"results":[{"id":"a"},{"id":"b"}]}), 2).unwrap(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn acknowledgment_counts_must_be_exact() {
        for value in [
            json!({}),
            json!({"deleted":true}),
            json!({"deleted":15}),
            json!({"deleted":17}),
        ] {
            assert!(count(&value, "deleted", 16).is_err());
        }
        assert!(count(&json!({"deleted":16}), "deleted", 16).is_ok());
    }

    #[test]
    fn deletion_checks_require_an_explicit_empty_array() {
        assert!(empty_results(json!({"results":[]})).is_ok());
        assert!(empty_results(json!({})).is_err());
        assert!(empty_results(json!({"results":[{"id":"stale"}]})).is_err());
    }

    #[test]
    fn semantic_failure_is_counted_as_a_request_failure() {
        let mut timings = Timings::default();
        let mut failures = Vec::new();
        let value = measured(
            &mut timings,
            &mut failures,
            "crud_get",
            || Ok(json!({})),
            |_| Err::<(), _>("updated value missing".into()),
        );
        assert!(value.is_none());
        assert_eq!(failures.len(), 1);
        let metrics = timings.finish();
        assert_eq!(metrics["crud_get"].requests, 1);
        assert_eq!(metrics["crud_get"].errors, 1);
    }
}
