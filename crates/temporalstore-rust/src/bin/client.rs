use temporalstore_rust::types::{
    Command, ExecuteRequest, FeatureFilter, FeatureFilterOp, FeaturePoint, SequenceFeatureRow,
};
use temporalstore_rust::TemporalStoreClient;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        usage();
        std::process::exit(2);
    }
    let proxy = std::env::var("TS_PROXY_ADDR").unwrap_or_else(|_| "127.0.0.1:17000".to_string());
    let shard_id = std::env::var("TS_SHARD_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let client = TemporalStoreClient::new(proxy);
    let command = match args[1].as_str() {
        "set" if args.len() == 4 => Command::StringSet {
            key: args[2].clone(),
            value: args[3].as_bytes().to_vec(),
        },
        "setex" if args.len() == 5 => Command::StringSetEx {
            key: args[2].clone(),
            value: args[3].as_bytes().to_vec(),
            ttl_ms: args[4].parse().expect("ttl_ms must be u64"),
        },
        "get" if args.len() == 3 => Command::StringGet {
            key: args[2].clone(),
        },
        "hset" if args.len() == 5 => Command::HashSet {
            key: args[2].clone(),
            field: args[3].clone(),
            value: args[4].as_bytes().to_vec(),
        },
        "hget" if args.len() == 4 => Command::HashGet {
            key: args[2].clone(),
            field: args[3].clone(),
        },
        "sadd" if args.len() == 4 => Command::SetAdd {
            key: args[2].clone(),
            member: args[3].as_bytes().to_vec(),
        },
        "smembers" if args.len() == 3 => Command::SetMembers {
            key: args[2].clone(),
        },
        "srem" if args.len() == 4 => Command::SetRemove {
            key: args[2].clone(),
            member: args[3].as_bytes().to_vec(),
        },
        "delete" if args.len() == 3 => Command::CommonDelete {
            key: args[2].clone(),
        },
        "expire" if args.len() == 4 => Command::CommonExpire {
            key: args[2].clone(),
            ttl_ms: args[3].parse().expect("ttl_ms must be u64"),
        },
        "ttl" if args.len() == 3 => Command::CommonTtl {
            key: args[2].clone(),
        },
        "fappend" if args.len() == 5 => Command::FeatureAppend {
            key: args[2].clone(),
            points: vec![FeaturePoint {
                timestamp_ms: args[3].parse().expect("timestamp must be u64"),
                value: args[4].as_bytes().to_vec(),
            }],
        },
        "fquery" if args.len() == 5 => Command::FeatureQuery {
            key: args[2].clone(),
            start_ms: args[3].parse().expect("start must be u64"),
            end_ms: args[4].parse().expect("end must be u64"),
            count: None,
        },
        "fquery" if args.len() == 6 => Command::FeatureQuery {
            key: args[2].clone(),
            start_ms: args[3].parse().expect("start must be u64"),
            end_ms: args[4].parse().expect("end must be u64"),
            count: Some(args[5].parse().expect("count must be usize")),
        },
        "freplace" if args.len() == 7 => Command::FeatureReplace {
            key: args[2].clone(),
            start_ms: args[3].parse().expect("start must be u64"),
            end_ms: args[4].parse().expect("end must be u64"),
            points: vec![FeaturePoint {
                timestamp_ms: args[5].parse().expect("timestamp must be u64"),
                value: args[6].as_bytes().to_vec(),
            }],
        },
        "fdel" if args.len() == 3 => Command::FeatureDelete {
            key: args[2].clone(),
        },
        "fagg" if args.len() == 6 => Command::FeatureAggQuery {
            key: args[2].clone(),
            start_ms: args[3].parse().expect("start must be u64"),
            end_ms: args[4].parse().expect("end must be u64"),
            aggregator: args[5].clone(),
            count: None,
        },
        "fagg" if args.len() == 7 => Command::FeatureAggQuery {
            key: args[2].clone(),
            start_ms: args[3].parse().expect("start must be u64"),
            end_ms: args[4].parse().expect("end must be u64"),
            aggregator: args[5].clone(),
            count: Some(args[6].parse().expect("count must be usize")),
        },
        "seqadd" if args.len() == 8 => Command::SequenceAdd {
            key: args[2].clone(),
            rows: vec![SequenceFeatureRow {
                timestamp_ms: args[3].parse().expect("timestamp must be u64"),
                gid: args[4].parse().expect("gid must be u64"),
                action_type: args[5].parse().expect("action_type must be u32"),
                duration: args[6].parse().expect("duration must be u32"),
                author_id: args[7].parse().expect("author_id must be u64"),
            }],
        },
        "seqquery" if args.len() == 6 => Command::SequenceQuery {
            key: args[2].clone(),
            start_ms: args[3].parse().expect("start must be u64"),
            end_ms: args[4].parse().expect("end must be u64"),
            count: args[5].parse().expect("count must be usize"),
            filters: Vec::new(),
        },
        "seqquery" if args.len() == 9 => Command::SequenceQuery {
            key: args[2].clone(),
            start_ms: args[3].parse().expect("start must be u64"),
            end_ms: args[4].parse().expect("end must be u64"),
            count: args[5].parse().expect("count must be usize"),
            filters: vec![FeatureFilter {
                field: args[6].clone(),
                op: parse_filter_op(&args[7]),
                value: args[8].parse().expect("filter value must be u64"),
            }],
        },
        "ipsadd" if args.len() == 5 => Command::IpsAdd {
            key: args[2].clone(),
            timestamp_ms: args[3].parse().expect("timestamp must be u64"),
            instance: args[4].as_bytes().to_vec(),
        },
        "ipslast" if args.len() == 4 => Command::IpsQueryLast {
            key: args[2].clone(),
            count: args[3].parse().expect("count must be usize"),
        },
        "riskinc" if args.len() == 5 => Command::RiskIncrement {
            key: args[2].clone(),
            timestamp_ms: args[3].parse().expect("timestamp must be u64"),
            amount: args[4].parse().expect("amount must be i64"),
        },
        "riskcount" if args.len() == 5 => Command::RiskCount {
            key: args[2].clone(),
            start_ms: args[3].parse().expect("start must be u64"),
            end_ms: args[4].parse().expect("end must be u64"),
        },
        _ => {
            usage();
            std::process::exit(2);
        }
    };
    let response = client
        .execute(ExecuteRequest { shard_id, command })
        .expect("request failed");
    println!("{}", serde_json::to_string_pretty(&response).unwrap());
}

fn usage() {
    eprintln!("usage:");
    eprintln!("  client set <key> <value>");
    eprintln!("  client setex <key> <value> <ttl_ms>");
    eprintln!("  client get <key>");
    eprintln!("  client hset <key> <field> <value>");
    eprintln!("  client hget <key> <field>");
    eprintln!("  client sadd <key> <member>");
    eprintln!("  client smembers <key>");
    eprintln!("  client srem <key> <member>");
    eprintln!("  client fappend <key> <timestamp_ms> <value>");
    eprintln!("  client fquery <key> <start_ms> <end_ms> [count]");
    eprintln!("  client freplace <key> <start_ms> <end_ms> <timestamp_ms> <value>");
    eprintln!("  client fdel <key>");
    eprintln!("  client fagg <key> <start_ms> <end_ms> <count|sum|min|max> [count]");
    eprintln!("  client delete <key>");
    eprintln!("  client expire <key> <ttl_ms>");
    eprintln!("  client ttl <key>");
    eprintln!("  client seqadd <key> <ts> <gid> <action_type> <duration> <author_id>");
    eprintln!("  client seqquery <key> <start> <end> <count> [field op value]");
    eprintln!("  client ipsadd <key> <timestamp_ms> <instance>");
    eprintln!("  client ipslast <key> <count>");
    eprintln!("  client riskinc <key> <timestamp_ms> <amount>");
    eprintln!("  client riskcount <key> <start_ms> <end_ms>");
}

fn parse_filter_op(op: &str) -> FeatureFilterOp {
    match op {
        "=" | "==" | "eq" => FeatureFilterOp::Equal,
        "!=" | "ne" => FeatureFilterOp::NotEqual,
        ">" | "gt" => FeatureFilterOp::GreaterThan,
        "<" | "lt" => FeatureFilterOp::LessThan,
        other => panic!("unsupported filter op: {other}"),
    }
}
