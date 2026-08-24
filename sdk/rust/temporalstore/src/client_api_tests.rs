// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[cfg(feature = "direct")]
use crate::Client;
#[cfg(feature = "proxy")]
use crate::ProxyClient;

#[cfg(feature = "direct")]
#[test]
fn direct_client_exposes_c_abi_parity_methods() {
    let _: fn(&Client, &str, &str) -> crate::Result<()> = Client::put_string;
    let _: fn(&Client, &str, &str, u64) -> crate::Result<()> = Client::put_string_with_ttl;
    let _: fn(&Client, &str) -> crate::Result<String> = Client::get_string;
    let _: fn(&Client, &str, &str, &str) -> crate::Result<()> = Client::hset;
    let _: fn(&Client, &str, &str) -> crate::Result<String> = Client::hget;
    let _: fn(&Client, &str) -> crate::Result<Vec<(String, String)>> = Client::hgetall;
    let _: fn(&Client, &str) -> crate::Result<Vec<(String, String)>> = Client::scan_hash;
    let _: fn(&Client, &str, &str) -> crate::Result<()> = Client::hdel;
    let _: fn(&Client, &str) -> crate::Result<()> = Client::delete_object;
    let _: fn(&Client, &str, u64) -> crate::Result<()> = Client::expire;
    let _: fn(&Client, &str) -> crate::Result<u64> = Client::ttl;
    let _: fn(&Client, &str, &str) -> crate::Result<()> = Client::sadd;
    let _: fn(&Client, &str) -> crate::Result<Vec<String>> = Client::smembers;
    let _: fn(&Client, &str, &[crate::FeaturePoint]) -> crate::Result<()> =
        Client::add_feature_points;
    let _: fn(&Client, &str, u64, u64, u64) -> crate::Result<Vec<crate::FeaturePoint>> =
        Client::query_feature_points;
    let _: fn(
        &Client,
        &str,
        u64,
        u64,
        u64,
        &[crate::FeatureFilter],
    ) -> crate::Result<Vec<crate::FeaturePoint>> = Client::query_feature_points_filtered;
    let _: fn(
        &Client,
        &str,
        i64,
        u64,
        crate::ControlStatePrecision,
        &str,
        u64,
    ) -> crate::Result<()> = Client::control_state_increment;
    let _: fn(
        &Client,
        &str,
        crate::ControlStatePrecision,
        crate::ControlStateWindow,
    ) -> crate::Result<i64> = Client::control_state_count;
    let _: fn(&Client, &[(&str, &str, &str)], Option<&str>, Option<&str>) -> crate::Result<()> =
        Client::matrixark_batch_append_records;
}

#[cfg(feature = "proxy")]
#[test]
fn proxy_client_exposes_proxy_parity_methods() {
    let _: fn(&ProxyClient, &str, &[crate::FeaturePoint]) -> crate::Result<()> =
        ProxyClient::feature_add;
    let _: fn(
        &ProxyClient,
        &str,
        u64,
        u64,
        Option<usize>,
    ) -> crate::Result<Vec<crate::FeaturePoint>> = ProxyClient::feature_query;
    let _: fn(
        &ProxyClient,
        &str,
        u64,
        u64,
        Option<usize>,
        &[crate::FeatureFilter],
    ) -> crate::Result<Vec<crate::FeaturePoint>> = ProxyClient::feature_query_filtered;
    let _: fn(&ProxyClient, &str, u64, u64, &str, Option<usize>) -> crate::Result<i64> =
        ProxyClient::feature_aggregate;
    let _: fn(
        &ProxyClient,
        &str,
        i64,
        u64,
        crate::ControlStatePrecision,
        &str,
        u64,
    ) -> crate::Result<()> = ProxyClient::control_state_increment;
    let _: fn(
        &ProxyClient,
        &str,
        crate::ControlStatePrecision,
        crate::ControlStateWindow,
    ) -> crate::Result<i64> = ProxyClient::control_state_count;
    let _: fn(&ProxyClient, &str, &str) -> crate::Result<()> = ProxyClient::set;
    let _: fn(&ProxyClient, &str) -> crate::Result<Option<String>> = ProxyClient::get;
    let _: fn(&ProxyClient, &str, u64, i64) -> crate::Result<()> =
        ProxyClient::control_state_hset;
    let _: fn(
        &ProxyClient,
        &str,
        &str,
        u64,
        crate::ControlStateHType,
        u64,
        crate::ControlStatePrecision,
    ) -> crate::Result<()> = ProxyClient::control_state_hset_with_options;
    let _: fn(
        &ProxyClient,
        &str,
        crate::ControlStatePrecision,
        crate::ControlStateWindow,
        &str,
    ) -> crate::Result<Vec<i64>> = ProxyClient::control_state_hquery;
    let _: fn(
        &ProxyClient,
        &str,
        &[&str],
        u64,
        u64,
        crate::ControlStatePrecision,
        bool,
    ) -> crate::Result<()> = ProxyClient::control_state_cpc_set;
    let _: fn(
        &ProxyClient,
        &str,
        crate::ControlStatePrecision,
        crate::ControlStateWindow,
    ) -> crate::Result<Vec<i64>> = ProxyClient::control_state_cpc_query;
    let _: fn(
        &ProxyClient,
        &str,
        &str,
        u64,
        u64,
        crate::ControlStateFolType,
    ) -> crate::Result<()> = ProxyClient::control_state_fol_set;
    let _: fn(&ProxyClient, &str) -> crate::Result<Option<String>> =
        ProxyClient::control_state_fol_query;
    let _: fn(&ProxyClient, &str) -> crate::Result<Vec<(String, String)>> =
        ProxyClient::control_state_manager;
    let _: fn(
        &ProxyClient,
        &str,
        &str,
        &[(&str, &str)],
        &str,
        &str,
        bool,
    ) -> crate::Result<Vec<(String, String)>> = ProxyClient::control_state_manager_with_options;
    let _: fn(&ProxyClient, &str, &str, &str) -> crate::Result<()> = ProxyClient::hset;
    let _: fn(&ProxyClient, &str, &str) -> crate::Result<String> = ProxyClient::hget;
    let _: fn(&ProxyClient, &str, &[(&str, &str)]) -> crate::Result<()> = ProxyClient::hmset;
    let _: fn(&ProxyClient, &str, &[&str]) -> crate::Result<Vec<Option<String>>> =
        ProxyClient::hmget;
    let _: fn(&ProxyClient, &str) -> crate::Result<Vec<(String, String)>> = ProxyClient::hgetall;
    let _: fn(&ProxyClient, &str) -> crate::Result<u64> = ProxyClient::hlen;
    let _: fn(&ProxyClient, &str, &str) -> crate::Result<()> = ProxyClient::hdel;
    let _: fn(&ProxyClient, &str) -> crate::Result<()> = ProxyClient::delete_object;
    let _: fn(&ProxyClient, &str, u64) -> crate::Result<()> = ProxyClient::expire;
    let _: fn(&ProxyClient, &str) -> crate::Result<u64> = ProxyClient::ttl;
    let _: fn(&ProxyClient, &str, &str) -> crate::Result<()> = ProxyClient::sadd;
    let _: fn(&ProxyClient, &str) -> crate::Result<Vec<String>> = ProxyClient::smembers;
}
