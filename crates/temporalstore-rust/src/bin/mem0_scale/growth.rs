// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub(super) fn complete_corpus(
    infer: bool,
    requested: usize,
    acknowledged: usize,
    memories: usize,
) -> bool {
    acknowledged == requested
        && if infer {
            memories > 0
        } else {
            memories == requested
        }
}

#[derive(Debug, Serialize)]
pub(super) struct Checkpoint {
    pub attempted_sources: usize,
    pub acknowledged_sources: usize,
    pub memory_records: usize,
    pub elapsed_ms: u128,
    pub api: BTreeMap<String, ApiMetric>,
    pub cumulative_service_processes: Vec<ProcessMetric>,
}

pub(super) fn parse_checkpoints(raw: Option<&str>, records: usize) -> Result<Vec<usize>, ()> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let mut values = raw
        .split(',')
        .map(|value| value.trim().parse::<usize>().map_err(|_| ()))
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty()
        || values.len() > 32
        || values.iter().any(|value| *value == 0 || *value > records)
    {
        return Err(());
    }
    values.sort_unstable();
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(());
    }
    Ok(values)
}

pub(super) fn search(
    options: &Options,
    tenant: &str,
    attempted: usize,
    ingested: usize,
    acknowledged: usize,
    timings: &mut Timings,
    failures: &mut Vec<String>,
) {
    for query_index in 0..options.searches {
        // Keep the oldest source in every checkpoint and probe the current tail too.
        let record_index = match query_index % 3 {
            0 => 0,
            1 => attempted - 1,
            _ => query_record_index(attempted, options.batch_size, query_index / 3),
        };
        let query_tenant = corpus_tenant(
            tenant,
            (record_index / options.batch_size) % options.tenant_shards,
        );
        let mut body = scope_body(&query_tenant);
        body["query"] = json!(format!("topic {record_index:08} retrieval marker"));
        body["limit"] = json!(10);
        if let Some(provider) = &options.provider {
            body["provider"] = provider.clone();
        }
        measure_post_checked(
            timings,
            failures,
            "search",
            &options.addr,
            "/memories/search",
            &body,
            |response| {
                if options.infer && ingested == 0 && acknowledged == attempted {
                    if response["results"].as_array().is_some_and(Vec::is_empty) {
                        Ok(())
                    } else {
                        Err("search returned memories when extraction produced no facts".into())
                    }
                } else {
                    validate_search(response, record_index, options.infer)
                }
            },
        );
    }
}

fn validate_search(response: &Value, record_index: usize, infer: bool) -> Result<(), String> {
    require_search_candidates(response, 10)?;
    if infer {
        return Ok(());
    }
    let marker = format!("topic {record_index:08} retrieval marker;");
    if response["results"].as_array().unwrap().iter().any(|row| {
        row["memory"]
            .as_str()
            .is_some_and(|text| text.contains(&marker))
    }) {
        Ok(())
    } else {
        Err(format!(
            "search did not retrieve expected source {record_index}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inferred_fact_counts_are_not_source_acknowledgements() {
        assert!(complete_corpus(true, 2, 2, 1));
        assert!(complete_corpus(true, 2, 2, 4));
        assert!(!complete_corpus(true, 2, 1, 2));
        assert!(!complete_corpus(true, 2, 2, 0));
        assert!(complete_corpus(false, 2, 2, 2));
        assert!(!complete_corpus(false, 2, 2, 1));
    }

    #[test]
    fn checkpoint_sizes_are_bounded_sorted_and_validated() {
        assert!(parse_checkpoints(None, 100).unwrap().is_empty());
        assert_eq!(
            parse_checkpoints(Some("100, 8,32"), 100).unwrap(),
            vec![8, 32, 100]
        );
        for raw in ["", "0", "101", "-1", "8,8", "1,", "x"] {
            assert!(parse_checkpoints(Some(raw), 100).is_err());
        }
        assert!(parse_checkpoints(
            Some(
                &(1..=33)
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            100
        )
        .is_err());
    }

    #[test]
    fn unrelated_nonempty_results_cannot_pass_explicit_search_quality() {
        let unrelated = json!({"results":[{"id":"wrong","memory":corpus_text(2, 512)}]});
        assert!(validate_search(&unrelated, 1, false).is_err());
        assert!(validate_search(&unrelated, 1, true).is_ok());
        let expected = json!({"results":[{"id":"right","memory":corpus_text(1, 512)}]});
        assert!(validate_search(&expected, 1, false).is_ok());
        assert!(validate_search(&json!({"results":[{"id":"missing-text"}]}), 1, false).is_err());
    }
}
