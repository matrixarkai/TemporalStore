use std::collections::BTreeMap;

use super::product_model::{control_state_family_key, control_state_family_name};
use super::state::ShardState;
use crate::types::{ControlStateFamily, ControlStateFolType};

pub(super) fn control_state_manager_entries(
    shard: &ShardState,
    key: &str,
    op_type: Option<&str>,
    field_list: &[(String, String)],
    start_offset: &str,
    end_offset: &str,
    is_cpc: bool,
) -> Vec<(String, Vec<u8>)> {
    match control_state_manager_op_code(op_type) {
        Some(2) => control_state_manager_query_entries(shard, key, field_list, is_cpc),
        Some(5) => {
            control_state_manager_field_list_entries(shard, key, start_offset, end_offset, is_cpc)
        }
        Some(6) => control_state_manager_field_count_entries(shard, key, is_cpc),
        Some(7) => control_state_manager_all_data_entries(shard, key, is_cpc),
        _ => control_state_manager_summary_entries(shard, key),
    }
}

fn control_state_manager_op_code(op_type: Option<&str>) -> Option<i64> {
    let value = op_type?.trim();
    value.parse::<i64>().ok().or_else(|| match value {
        "QUERY" | "query" => Some(2),
        "FIELD_LIST" | "field_list" => Some(5),
        "FIELD_COUNT" | "field_count" => Some(6),
        "ALL_DATA_VALUE" | "all_data_value" => Some(7),
        _ => None,
    })
}

fn control_state_manager_series_key(key: &str, is_cpc: bool) -> String {
    control_state_family_key(
        if is_cpc {
            ControlStateFamily::Cpc
        } else {
            ControlStateFamily::H
        },
        key,
    )
}

fn control_state_manager_series<'a>(
    shard: &'a ShardState,
    key: &str,
    is_cpc: bool,
) -> Option<&'a BTreeMap<u64, i64>> {
    shard
        .control_state
        .get(&control_state_manager_series_key(key, is_cpc))
}

fn control_state_manager_query_entries(
    shard: &ShardState,
    key: &str,
    field_list: &[(String, String)],
    is_cpc: bool,
) -> Vec<(String, Vec<u8>)> {
    let Some(series) = control_state_manager_series(shard, key, is_cpc) else {
        return Vec::new();
    };
    field_list
        .iter()
        .filter_map(|(field, _)| {
            field
                .parse::<u64>()
                .ok()
                .and_then(|timestamp_ms| series.get(&timestamp_ms))
                .map(|value| (field.clone(), value.to_string().into_bytes()))
        })
        .collect()
}

fn control_state_manager_field_list_entries(
    shard: &ShardState,
    key: &str,
    start_offset: &str,
    end_offset: &str,
    is_cpc: bool,
) -> Vec<(String, Vec<u8>)> {
    let Some(series) = control_state_manager_series(shard, key, is_cpc) else {
        return vec![("key_list".to_string(), Vec::new())];
    };
    let start = start_offset.parse::<u64>().unwrap_or(0);
    let end = end_offset.parse::<u64>().unwrap_or(u64::MAX);
    let value = series
        .range(start..=end)
        .map(|(timestamp_ms, _)| timestamp_ms.to_string())
        .collect::<Vec<_>>()
        .join(",");
    vec![("key_list".to_string(), value.into_bytes())]
}

fn control_state_manager_field_count_entries(
    shard: &ShardState,
    key: &str,
    is_cpc: bool,
) -> Vec<(String, Vec<u8>)> {
    let size = control_state_manager_series(shard, key, is_cpc)
        .map(BTreeMap::len)
        .unwrap_or_default();
    vec![("size".to_string(), size.to_string().into_bytes())]
}

fn control_state_manager_all_data_entries(
    shard: &ShardState,
    key: &str,
    is_cpc: bool,
) -> Vec<(String, Vec<u8>)> {
    control_state_manager_series(shard, key, is_cpc)
        .map(|series| {
            series
                .iter()
                .map(|(timestamp_ms, value)| {
                    (timestamp_ms.to_string(), value.to_string().into_bytes())
                })
                .collect()
        })
        .unwrap_or_default()
}

fn control_state_manager_summary_entries(shard: &ShardState, key: &str) -> Vec<(String, Vec<u8>)> {
    let mut entries = Vec::new();
    for family in [
        ControlStateFamily::H,
        ControlStateFamily::Cpc,
        ControlStateFamily::Fol,
    ] {
        let family_key = control_state_family_key(family, key);
        let values = shard
            .control_state
            .get(&family_key)
            .map(|series| series.values().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        entries.push((
            format!("{}_events", control_state_family_name(family)),
            values.len().to_string().into_bytes(),
        ));
        entries.push((
            format!("{}_sum", control_state_family_name(family)),
            values.iter().sum::<i64>().to_string().into_bytes(),
        ));
    }
    if let Some(fol) = shard.control_state_fol.get(key) {
        entries.push(("fol_value".to_string(), fol.value.clone()));
        entries.push((
            "fol_occur_time_ms".to_string(),
            fol.occur_time_ms.to_string().into_bytes(),
        ));
        entries.push((
            "fol_type".to_string(),
            match fol.fol_type {
                ControlStateFolType::First => b"first".to_vec(),
                ControlStateFolType::Last => b"last".to_vec(),
            },
        ));
    }
    entries
}

pub(super) fn control_state_bucket_ms(timestamp_ms: u64, precision_ms: Option<u64>) -> u64 {
    precision_ms
        .filter(|precision_ms| *precision_ms > 0)
        .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
        .unwrap_or(timestamp_ms)
}
