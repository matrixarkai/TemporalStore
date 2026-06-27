use super::reports::{CppGoldenCaseReport, CppGoldenCorpusReport};
use crate::engine::TemporalEngine;
use crate::types::{
    parse_cpp_feature_filters, Command, CommandResponse, ExecuteRequest, FeatureFilterOp,
    FeaturePoint, RiskFamily, RiskFolType, SequenceFeatureRow,
};

pub fn cpp_feature_sequence_golden_corpus_report() -> CppGoldenCorpusReport {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut cases = Vec::new();

    let matching = SequenceFeatureRow {
        timestamp_ms: 1_000,
        gid: 42,
        action_type: 7,
        duration: 33,
        author_id: 9_001,
    };
    let replacement = SequenceFeatureRow {
        timestamp_ms: 1_001,
        gid: 43,
        action_type: 7,
        duration: 34,
        author_id: 9_002,
    };
    let non_matching = SequenceFeatureRow {
        timestamp_ms: 1_002,
        gid: 44,
        action_type: 8,
        duration: 35,
        author_id: 9_003,
    };

    record_golden_case(
        &mut cases,
        "cpp_feature_proto_roundtrip",
        SequenceFeatureRow::decode_cpp_feature_value(
            matching.timestamp_ms,
            &matching.encode_cpp_feature_value(),
        ) == Some(matching.clone()),
        "C++ feature protobuf fields gid/action_type/duration/author_id round-trip",
    );

    let duplicate_filters = parse_cpp_feature_filters(["gid = 42", "duration > 30", "gid != 42"]);
    record_golden_case(
        &mut cases,
        "cpp_feature_filter_last_field_wins",
        matches!(duplicate_filters, Ok(ref filters) if filters.len() == 2
            && filters[0].field == "gid"
            && filters[0].op == FeatureFilterOp::NotEqual
            && filters[0].value == 42
            && filters[1].field == "duration"),
        "C++ duplicate filter fields replace the previous field predicate",
    );

    let append = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "cpp-golden-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: matching.timestamp_ms,
                    value: matching.encode_cpp_feature_value(),
                },
                FeaturePoint {
                    timestamp_ms: replacement.timestamp_ms,
                    value: replacement.encode_cpp_feature_value(),
                },
                FeaturePoint {
                    timestamp_ms: non_matching.timestamp_ms,
                    value: non_matching.encode_cpp_feature_value(),
                },
            ],
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_feature_append_status",
        append.status.ok,
        "C++ feature points append through the Rust engine",
    );

    let filtered = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQueryFiltered {
            key: "cpp-golden-feature".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: Some(10),
            filters: parse_cpp_feature_filters(["action_type = 7", "duration <= 34"])
                .unwrap_or_default(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_feature_filtered_query",
        matches!(
            filtered.response,
            CommandResponse::FeaturePoints { ref points }
                if points.iter().map(|point| point.timestamp_ms).collect::<Vec<_>>()
                    == vec![matching.timestamp_ms, replacement.timestamp_ms]
        ),
        "C++ protobuf feature filters select matching timestamp/value rows",
    );

    let aggregate = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAggQuery {
            key: "cpp-golden-aggregate".to_string(),
            start_ms: 0,
            end_ms: 10,
            aggregator: "sum".to_string(),
            count: None,
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_feature_empty_sum_aggregate",
        aggregate.response == CommandResponse::Aggregate { value: 0 },
        "Empty C++ feature aggregate returns neutral zero",
    );

    let rows = vec![matching.clone(), replacement.clone(), non_matching.clone()];
    let add_rows = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: "cpp-golden-sequence".to_string(),
            rows: rows.clone(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_sequence_add_status",
        add_rows.status.ok,
        "C++ sequence rows append through timestamped KV pages",
    );

    let sequence_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceQuery {
            key: "cpp-golden-sequence".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: 10,
            filters: parse_cpp_feature_filters(["gid >= 42", "action_type = 7"])
                .unwrap_or_default(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_sequence_filtered_query",
        sequence_query.response
            == CommandResponse::SequenceRows {
                rows: vec![matching, replacement],
            },
        "C++ sequence filters reuse the feature predicate semantics",
    );

    let page_layout = engine.storage_recovery_report(1).feature_page_layout;
    record_golden_case(
        &mut cases,
        "cpp_timestamped_kv_shared_page_layout",
        page_layout.packed_feature_pages >= 1
            && page_layout.unique_feature_page_refs < page_layout.indexed_feature_points
            && !page_layout.has_errors(),
        "Timestamped feature/sequence values share packed pages without layout errors",
    );

    let total_cases = cases.len();
    let passed_cases = cases.iter().filter(|case| case.passed).count();
    CppGoldenCorpusReport {
        corpus: "feature_sequence_cpp_proto_v1".to_string(),
        total_cases,
        passed_cases,
        failed_cases: total_cases.saturating_sub(passed_cases),
        cases,
    }
}

pub fn cpp_api_golden_corpus_report() -> CppGoldenCorpusReport {
    let feature_report = cpp_feature_sequence_golden_corpus_report();
    let mut cases = feature_report.cases;
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let string_set = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "redis-string".to_string(),
            value: b"value".to_vec(),
        },
    });
    let string_get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "redis-string".to_string(),
        },
    });
    let hash_set = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "redis-hash".to_string(),
            field: "field".to_string(),
            value: b"hash-value".to_vec(),
        },
    });
    let hash_get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: "redis-hash".to_string(),
            field: "field".to_string(),
        },
    });
    let set_add = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetAdd {
            key: "redis-set".to_string(),
            member: b"member".to_vec(),
        },
    });
    let set_members = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: "redis-set".to_string(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_redis_string_hash_set_core",
        string_set.status.ok
            && string_get.response
                == CommandResponse::Bytes {
                    value: Some(b"value".to_vec()),
                }
            && hash_set.status.ok
            && hash_get.response
                == CommandResponse::Bytes {
                    value: Some(b"hash-value".to_vec()),
                }
            && set_add.status.ok
            && set_members.response
                == CommandResponse::Members {
                    members: vec![b"member".to_vec()],
                },
        "Redis-compatible string, hash, and set command shapes match expected C++ core behavior",
    );

    let common_exists = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExists {
            key: "redis-string".to_string(),
        },
    });
    let common_expire = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: "redis-string".to_string(),
            ttl_ms: 30_000,
        },
    });
    let common_ttl = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonTtl {
            key: "redis-string".to_string(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_common_exists_expire_ttl",
        common_exists.response == CommandResponse::Integer { value: 1 }
            && common_expire.status.ok
            && matches!(common_ttl.response, CommandResponse::Integer { value } if value > 0),
        "Common/Redis-compatible key existence and lifetime commands remain observable through the engine",
    );

    for (timestamp_ms, action_type, table_id, value) in [
        (100, 7, 11, b"ips-a".to_vec()),
        (200, 8, 11, b"ips-b".to_vec()),
    ] {
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsAddWithOptions {
                key: "ips-golden".to_string(),
                timestamp_ms,
                instance: value,
                action_type: Some(action_type),
                table_id: Some(table_id),
                request_id: Some(format!("req-{timestamp_ms}")),
            },
        });
    }
    let ips_filter = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::IpsFilter {
            key: "ips-golden".to_string(),
            start_ms: 0,
            end_ms: 500,
            count: None,
            action_type: Some(7),
            table_id: Some(11),
        },
    });
    let ips_stat = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::IpsStat {
            key: "ips-golden".to_string(),
            start_ms: 0,
            end_ms: 500,
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_ips_filter_and_stat",
        matches!(ips_filter.response, CommandResponse::FeaturePoints { ref points }
            if points == &vec![FeaturePoint { timestamp_ms: 100, value: b"ips-a".to_vec() }])
            && matches!(ips_stat.response, CommandResponse::IpsStats { ref stats }
                if stats.total == 2
                    && stats.first_timestamp_ms == Some(100)
                    && stats.last_timestamp_ms == Some(200)
                    && stats.action_type_counts == vec![(7, 1), (8, 1)]
                    && stats.table_id_counts == vec![(11, 2)]),
        "IPS option filters and stats match C++ action/table metadata behavior",
    );

    let ips_snapshot = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::IpsSnapshot {
            key: "ips-golden".to_string(),
            start_ms: 0,
            end_ms: 500,
            count: Some(10),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_ips_snapshot_range_order",
        matches!(ips_snapshot.response, CommandResponse::FeaturePoints { ref points }
            if points.iter().map(|point| point.timestamp_ms).collect::<Vec<_>>() == vec![100, 200]),
        "IPS snapshot/range queries return timestamp ordered points",
    );

    for (family, timestamp_ms, amount) in [
        (RiskFamily::H, 10, 5),
        (RiskFamily::H, 20, 7),
        (RiskFamily::Cpc, 10, 3),
    ] {
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskSet {
                family,
                key: "risk-golden".to_string(),
                timestamp_ms,
                amount,
            },
        });
    }
    let risk_sum = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskFamilyQuery {
            family: RiskFamily::H,
            key: "risk-golden".to_string(),
            start_ms: 0,
            end_ms: 100,
            aggregator: "sum".to_string(),
        },
    });
    let risk_cpc = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskSetAndGet {
            family: RiskFamily::Cpc,
            key: "risk-golden".to_string(),
            timestamp_ms: 20,
            amount: 4,
            start_ms: 0,
            end_ms: 100,
            aggregator: "sum".to_string(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_risk_family_aggregates",
        risk_sum.response == CommandResponse::Integer { value: 12 }
            && risk_cpc.response == CommandResponse::Integer { value: 7 },
        "Risk H/CPC/FOL family aggregate command shapes preserve C++ local semantics",
    );

    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskFolSet {
            key: "risk-golden".to_string(),
            value: b"late-first".to_vec(),
            occur_time_ms: 200,
            ttl_ms: 0,
            fol_type: RiskFolType::First,
        },
    });
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskFolSet {
            key: "risk-golden".to_string(),
            value: b"early-first".to_vec(),
            occur_time_ms: 100,
            ttl_ms: 0,
            fol_type: RiskFolType::First,
        },
    });
    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskFolQuery {
            key: "risk-golden".to_string(),
        },
    });
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskFolSet {
            key: "risk-golden".to_string(),
            value: b"latest".to_vec(),
            occur_time_ms: 300,
            ttl_ms: 0,
            fol_type: RiskFolType::Last,
        },
    });
    let last = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskFolQuery {
            key: "risk-golden".to_string(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_risk_fol_first_last",
        first.response
            == CommandResponse::Bytes {
                value: Some(b"early-first".to_vec()),
            }
            && last.response
                == CommandResponse::Bytes {
                    value: Some(b"latest".to_vec()),
                },
        "Risk FOL first/last selection follows occur-time ordering",
    );

    let manager = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::RiskManager {
            key: "risk-golden".to_string(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_risk_manager_summary",
        matches!(manager.response, CommandResponse::HashEntries { ref entries }
            if entries.iter().any(|(field, value)| field == "h_sum" && value.as_slice() == b"12")
                && entries.iter().any(|(field, value)| field == "cpc_sum" && value.as_slice() == b"7")
                && entries.iter().any(|(field, value)| field == "fol_value" && value.as_slice() == b"latest")),
        "Risk manager summary exposes family counts/sums and FOL metadata",
    );

    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "admin-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 1,
                    value: b"one".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 2,
                    value: b"two".to_vec(),
                },
            ],
        },
    });
    let storage_readiness = engine.storage_production_readiness_report(1);
    record_golden_case(
        &mut cases,
        "cpp_admin_storage_readiness_report",
        storage_readiness.production_ready
            && storage_readiness.block_store_bytes_written > 0
            && storage_readiness.feature_page_layout.packed_feature_pages >= 1,
        "Admin/storage readiness report is queryable after mixed C++ API corpus writes",
    );

    let total_cases = cases.len();
    let passed_cases = cases.iter().filter(|case| case.passed).count();
    CppGoldenCorpusReport {
        corpus: "cpp_api_golden_corpus_v1".to_string(),
        total_cases,
        passed_cases,
        failed_cases: total_cases.saturating_sub(passed_cases),
        cases,
    }
}

fn record_golden_case(
    cases: &mut Vec<CppGoldenCaseReport>,
    name: &str,
    passed: bool,
    detail: &str,
) {
    cases.push(CppGoldenCaseReport {
        name: name.to_string(),
        passed,
        detail: detail.to_string(),
    });
}
