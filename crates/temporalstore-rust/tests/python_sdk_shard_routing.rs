// SPDX-License-Identifier: Apache-2.0

use serde::Deserialize;
use temporalstore_rust::client::{crc64_jones, shard_id_for_key};

#[derive(Deserialize)]
struct RoutingFixture {
    first_shard_id: u64,
    shard_counts: [u64; 4],
    crc_vector: (String, String),
    keys: Vec<(String, u64, u64, u64, u64)>,
}

#[test]
fn python_sdk_routing_fixture_matches_rust_sdk() {
    let fixture: RoutingFixture = serde_json::from_str(include_str!(
        "fixtures/python_sdk_shard_routing.json"
    ))
    .unwrap();
    assert_eq!(
        format!("{:016x}", crc64_jones(fixture.crc_vector.0.as_bytes())),
        fixture.crc_vector.1
    );
    for (key, a, b, c, d) in fixture.keys {
        for (count, shard_id) in fixture.shard_counts.into_iter().zip([a, b, c, d]) {
            assert_eq!(
                shard_id_for_key(&key, fixture.first_shard_id, count, 1),
                shard_id,
                "key={key:?} count={count}"
            );
        }
    }
}
