use temporalstore::{
    FeatureFilter, FeatureFilterOp, ProxyClient, ProxyOptions, SequenceFeatureRow,
};

fn main() -> temporalstore::Result<()> {
    let client = ProxyClient::connect(ProxyOptions::new(
        "http://127.0.0.1:8080",
        "sdk_ns",
        "sdk_table",
    ));

    client.put_string("rust:proxy:user:42", r#"{"tier":"gold"}"#)?;
    println!("profile={}", client.get_string("rust:proxy:user:42")?);

    let key = "rust:proxy:user:42:sequence";
    client.add_sequence_feature_rows(
        key,
        &[
            SequenceFeatureRow {
                timestamp: 1700000000000,
                gid: 900,
                action_type: 1,
                duration: 31,
                author_id: 7000,
            },
            SequenceFeatureRow {
                timestamp: 1700000001000,
                gid: 901,
                action_type: 3,
                duration: 120,
                author_id: 7001,
            },
        ],
    )?;

    let rows = client.query_sequence_feature_rows(
        key,
        1700000000000,
        1700000002000,
        10,
        &[FeatureFilter {
            field: "action_type".to_string(),
            op: FeatureFilterOp::Equal,
            value: 3,
        }],
    )?;
    println!("rows={rows:?}");
    Ok(())
}
