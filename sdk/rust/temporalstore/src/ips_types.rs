// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct IpsFeatureStat {
    pub id: i64,
    pub slot: i32,
    pub has_slot: bool,
    #[cfg_attr(feature = "proxy", serde(rename = "type"))]
    pub kind: i32,
    pub v1: i32,
    pub v2: i32,
}

#[derive(Clone, Debug)]
pub struct IpsInstance {
    pub table: String,
    pub uid: i64,
    pub timestamp_us: i64,
    pub action_type: i32,
    pub logical_table: i32,
    pub features: Vec<IpsFeatureStat>,
}

impl Default for IpsInstance {
    fn default() -> Self {
        Self {
            table: "table_compress".to_string(),
            uid: 0,
            timestamp_us: 0,
            action_type: 0,
            logical_table: 0,
            features: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IpsLastQuery {
    pub table: String,
    pub uid: i64,
    pub action_type: i32,
    pub logical_table: i32,
    pub slot: i32,
    pub top_k: i32,
    pub last_instances: i64,
}

impl Default for IpsLastQuery {
    fn default() -> Self {
        Self {
            table: "table_compress".to_string(),
            uid: 0,
            action_type: 0,
            logical_table: 0,
            slot: 0,
            top_k: 20,
            last_instances: 10,
        }
    }
}
