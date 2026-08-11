// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Shared test helpers/fixtures, split from tests.rs.
#![allow(dead_code)]
use super::*;

pub(super) fn key_for_shard(table: &TemporalStoreTable, shard_id: ShardId) -> String {
    (0..10_000)
        .map(|index| format!("key-{shard_id}-{index}"))
        .find(|key| table.shard_id_for_key(key) == shard_id)
        .unwrap()
}

pub(super) fn free_local_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

pub(super) fn wait_for_http(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server {addr} did not start");
}

