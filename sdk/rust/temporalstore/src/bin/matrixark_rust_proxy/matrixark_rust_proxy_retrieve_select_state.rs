// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::matrixark_rust_proxy_candidates::record_node_hash;
use crate::matrixark_rust_proxy_retrieve_result::RetrieveSelection;

pub(crate) struct RetrieveSelectState {
    pub(crate) selected: Vec<Value>,
    pub(crate) selected_signatures: HashSet<String>,
    pub(crate) selected_counts: HashMap<String, u64>,
    pub(crate) selected_nodes: HashSet<u64>,
    pub(crate) used_tokens: u64,
    pub(crate) cross_used_tokens: u64,
    pub(crate) cross_selected_refs: u64,
    pub(crate) entity_bridge_selected_refs: u64,
    pub(crate) selected_cross_sessions: HashSet<String>,
    pub(crate) dropped_over_budget: u64,
    pub(crate) dropped_cross_budget: u64,
    pub(crate) dropped_cross_session_cap: u64,
    pub(crate) dropped_cross_candidate_cap: u64,
    pub(crate) dropped_entity_bridge_slot_reserved: u64,
    pub(crate) dropped_low_score: u64,
    pub(crate) dropped_policy_ref: u64,
    pub(crate) dropped_duplicate_ref: u64,
}

impl RetrieveSelectState {
    pub(crate) fn new() -> Self {
        Self {
            selected: Vec::new(),
            selected_signatures: HashSet::new(),
            selected_counts: HashMap::new(),
            selected_nodes: HashSet::new(),
            used_tokens: 0,
            cross_used_tokens: 0,
            cross_selected_refs: 0,
            entity_bridge_selected_refs: 0,
            selected_cross_sessions: HashSet::new(),
            dropped_over_budget: 0,
            dropped_cross_budget: 0,
            dropped_cross_session_cap: 0,
            dropped_cross_candidate_cap: 0,
            dropped_entity_bridge_slot_reserved: 0,
            dropped_low_score: 0,
            dropped_policy_ref: 0,
            dropped_duplicate_ref: 0,
        }
    }

    pub(crate) fn select_node(&mut self, record: &Value, context_class: &str) {
        *self
            .selected_counts
            .entry(context_class.to_string())
            .or_default() += 1;
        if let Some(node_hash) = record_node_hash(record) {
            self.selected_nodes.insert(node_hash);
        }
    }

    pub(crate) fn into_selection(self) -> RetrieveSelection {
        RetrieveSelection {
            selected: self.selected,
            selected_counts: self.selected_counts,
            selected_nodes: self.selected_nodes,
            used_tokens: self.used_tokens,
            cross_used_tokens: self.cross_used_tokens,
            cross_selected_refs: self.cross_selected_refs,
            entity_bridge_selected_refs: self.entity_bridge_selected_refs,
            selected_cross_sessions: self.selected_cross_sessions,
            dropped_over_budget: self.dropped_over_budget,
            dropped_cross_budget: self.dropped_cross_budget,
            dropped_cross_session_cap: self.dropped_cross_session_cap,
            dropped_cross_candidate_cap: self.dropped_cross_candidate_cap,
            dropped_entity_bridge_slot_reserved: self.dropped_entity_bridge_slot_reserved,
            dropped_low_score: self.dropped_low_score,
            dropped_policy_ref: self.dropped_policy_ref,
            dropped_duplicate_ref: self.dropped_duplicate_ref,
        }
    }
}
