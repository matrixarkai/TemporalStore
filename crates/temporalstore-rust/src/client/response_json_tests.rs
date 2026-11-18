// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn http_hash_entries_match_the_public_decoder() {
    let response = ExecuteResponse {
        status: Status::ok(),
        response: CommandResponse::HashEntries {
            entries: vec![
                ("field".into(), vec![0, 1, 127, 128, 255]),
                ("empty".into(), vec![]),
            ],
        },
    };
    let json = serde_json::to_vec(&response).unwrap();
    let decoded: ExecuteResponse = serde_json::from_slice::<ExecuteJson>(&json).unwrap().into();
    assert_eq!(decoded, response);
    let value = serde_json::to_value(&response).unwrap();
    let decoded: ExecuteResponse = serde_json::from_value::<ExecuteJson>(value).unwrap().into();
    assert_eq!(decoded, response);
}

#[test]
fn http_hash_entries_accept_tag_last_and_unknown_fields() {
    let response = br#"{"status":{"ok":true,"code":"ok","message":""},"response":{"entries":[["field",[0,255]]],"extra":[1,2,3],"kind":"hash_entries"}}"#;
    let expected: ExecuteResponse = serde_json::from_slice(response).unwrap();
    let actual: ExecuteResponse = serde_json::from_slice::<ExecuteJson>(response)
        .unwrap()
        .into();
    assert_eq!(actual, expected);
}

#[test]
fn http_hash_entries_reject_invalid_payloads_like_the_public_decoder() {
    for response in [
        r#"{"kind":"hash_entries"}"#,
        r#"{"kind":"hash_entries","entries":[["a",[-1]]]}"#,
        r#"{"kind":"hash_entries","entries":[["a",[256]]]}"#,
        r#"{"kind":"hash_entries","entries":[["a","bytes"]]}"#,
        r#"{"kind":"hash_entries","entries":[["a",[],0]]}"#,
        r#"{"kind":"hash_entries","entries":[],"entries":[]}"#,
        r#"{"kind":"hash_entries","kind":"empty","entries":[]}"#,
        r#"{"kind":"unknown","entries":[]}"#,
    ] {
        let json =
            format!(r#"{{"status":{{"ok":true,"code":"ok","message":""}},"response":{response}}}"#);
        assert!(
            serde_json::from_str::<ExecuteResponse>(&json).is_err(),
            "{response}"
        );
        assert!(
            serde_json::from_str::<ExecuteJson>(&json).is_err(),
            "{response}"
        );
    }
}

#[test]
fn http_batch_decoder_keeps_statuses_and_other_variants() {
    let responses = vec![
        CommandResponse::Empty,
        CommandResponse::Bytes { value: None },
        CommandResponse::Bytes {
            value: Some(vec![0, 255]),
        },
        CommandResponse::Values {
            values: vec![None, Some(vec![]), Some(vec![255])],
        },
        CommandResponse::Integer { value: -7 },
        CommandResponse::Members {
            members: vec![vec![0, 255], vec![]],
        },
        CommandResponse::HashEntries {
            entries: vec![("field".into(), vec![255])],
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(index, response)| ExecuteResponse {
        status: if index == 0 {
            Status::error("applied_unknown", "not acknowledged")
        } else {
            Status::ok()
        },
        response,
    })
    .collect();
    let expected = BatchExecuteResponse {
        status: Status::error("partial", "child failed"),
        responses,
    };
    let json = serde_json::to_vec(&expected).unwrap();
    let actual: BatchExecuteResponse = serde_json::from_slice::<BatchJson>(&json).unwrap().into();
    assert_eq!(actual, expected);
    let packed = rmp_serde::to_vec_named(&expected).unwrap();
    let actual: BatchExecuteResponse = rmp_serde::from_slice::<BatchJson>(&packed).unwrap().into();
    assert_eq!(actual, expected);
}

#[test]
fn http_batch_decoder_handles_byte_heavy_catalogs() {
    let expected = BatchExecuteResponse {
        status: Status::ok(),
        responses: vec![ExecuteResponse {
            status: Status::ok(),
            response: CommandResponse::HashEntries {
                entries: (0..1_024)
                    .map(|index| (format!("memory-{index}"), vec![(index % 256) as u8; 128]))
                    .collect(),
            },
        }],
    };
    let json = serde_json::to_vec(&expected).unwrap();
    let actual: BatchExecuteResponse = serde_json::from_slice::<BatchJson>(&json).unwrap().into();
    assert_eq!(actual, expected);
}
