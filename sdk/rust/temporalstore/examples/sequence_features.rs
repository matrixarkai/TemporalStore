use temporalstore::{Client, FeatureFilter, FeatureFilterOp, Options, SequenceFeatureRow};

fn main() -> temporalstore::Result<()> {
    let mut options = Options::new("127.0.0.1:18200", "sdk_ns", "sdk_table");
    options.psm = "temporalstore.rust.example".to_string();

    let client = Client::connect(options)?;
    let key = "rust:user:42:sequence";
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
