use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::client::{slot_id_for_key, stable_key_hash};
use crate::types::{
    parse_cpp_feature_filters, Command, CommandResponse, ExecuteRequest, FeatureFilter,
    FeatureFilterOp, FeaturePoint, FeatureWritePolicy, RiskFamily, RiskFolType, ShardId,
    StringSetCondition,
};
use crate::TemporalStoreClient;

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<RespValue>),
}

impl RespValue {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.write_to(&mut out).expect("vec write cannot fail");
        out
    }

    fn write_to(&self, writer: &mut impl Write) -> io::Result<()> {
        match self {
            RespValue::SimpleString(value) => write!(writer, "+{value}\r\n"),
            RespValue::Error(value) => write!(writer, "-{value}\r\n"),
            RespValue::Integer(value) => write!(writer, ":{value}\r\n"),
            RespValue::Bulk(Some(value)) => {
                write!(writer, "${}\r\n", value.len())?;
                writer.write_all(value)?;
                writer.write_all(b"\r\n")
            }
            RespValue::Bulk(None) => writer.write_all(b"$-1\r\n"),
            RespValue::Array(values) => {
                write!(writer, "*{}\r\n", values.len())?;
                for value in values {
                    value.write_to(writer)?;
                }
                Ok(())
            }
        }
    }
}

pub fn serve_redis_proxy(addr: &str, proxy_addr: String, shard_id: ShardId) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    let client = Arc::new(TemporalStoreClient::new(proxy_addr));
    for stream in listener.incoming() {
        let stream = stream?;
        let client = Arc::clone(&client);
        thread::spawn(move || {
            let _ = handle_stream(stream, &client, shard_id);
        });
    }
    Ok(())
}

fn handle_stream(
    mut stream: TcpStream,
    client: &TemporalStoreClient,
    shard_id: ShardId,
) -> io::Result<()> {
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut state = RedisCommandState::default();
    while let Some(args) = read_command(&mut reader)? {
        let response = execute_redis_command_with_state(args, shard_id, &mut state, |command| {
            client
                .execute(ExecuteRequest { shard_id, command })
                .map_err(|err| err.to_string())
                .and_then(|response| {
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(format!(
                            "{} {}",
                            response.status.code, response.status.message
                        ))
                    }
                })
        });
        stream.write_all(&response.encode())?;
        stream.flush()?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisCommandState {
    pub config: HashMap<String, String>,
    pub master: Option<(String, String)>,
    pub authenticated: bool,
    pub loaded_shard_id: Option<ShardId>,
}

impl Default for RedisCommandState {
    fn default() -> Self {
        let mut config = HashMap::new();
        config.insert("requirepass".to_string(), String::new());
        config.insert("maxmemory".to_string(), "0".to_string());
        config.insert("maxmemory-policy".to_string(), "noeviction".to_string());
        Self {
            config,
            master: None,
            authenticated: false,
            loaded_shard_id: None,
        }
    }
}

pub fn execute_redis_command(
    args: Vec<Vec<u8>>,
    shard_id: ShardId,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let mut state = RedisCommandState::default();
    execute_redis_command_with_state(args, shard_id, &mut state, |command| execute(command))
}

pub fn execute_redis_command_with_state(
    args: Vec<Vec<u8>>,
    shard_id: ShardId,
    state: &mut RedisCommandState,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR empty command".to_string());
    }
    let command = upper(&args[0]);
    match command.as_str() {
        "AUTH" if args.len() == 2 => {
            let configured = state
                .config
                .get("requirepass")
                .map(String::as_str)
                .unwrap_or_default();
            if configured.is_empty() || configured.as_bytes() == args[1].as_slice() {
                state.authenticated = true;
                RespValue::SimpleString("OK".to_string())
            } else {
                RespValue::Error("ERR invalid password".to_string())
            }
        }
        "PING" => RespValue::SimpleString(
            args.get(1)
                .map(|value| String::from_utf8_lossy(value).to_string())
                .unwrap_or_else(|| "PONG".to_string()),
        ),
        "ECHO" if args.len() == 2 => RespValue::Bulk(Some(args[1].clone())),
        "SELECT" if args.len() == 2 => match parse_u64(&args[1], "db") {
            Ok(0) => RespValue::SimpleString("OK".to_string()),
            Ok(_) => RespValue::Error("ERR DB index is out of range".to_string()),
            Err(err) => RespValue::Error(err),
        },
        "BGSAVE" if args.len() == 1 || args.len() == 2 => {
            RespValue::SimpleString("Background saving started".to_string())
        }
        "CONFIG" if args.len() >= 2 => redis_config_response(&args, state),
        "SLAVEOF" if args.len() == 3 => {
            let host = string_arg(&args[1]);
            let port = string_arg(&args[2]);
            if host.eq_ignore_ascii_case("no") && port.eq_ignore_ascii_case("one") {
                state.master = None;
            } else {
                state.master = Some((host, port));
            }
            RespValue::SimpleString("OK".to_string())
        }
        "INFO" if args.len() == 1 || args.len() == 2 => {
            let section = args
                .get(1)
                .map(|value| string_arg(value))
                .unwrap_or_else(|| "default".to_string());
            RespValue::Bulk(Some(
                redis_info(section.as_str(), shard_id, state).into_bytes(),
            ))
        }
        "PARTITION" if args.len() >= 2 => redis_partition_response(&args, shard_id, state),
        "PSLOTHASHKEY" if args.len() == 2 => {
            RespValue::Integer(slot_id_for_key(String::from_utf8_lossy(&args[1]).as_ref()) as i64)
        }
        "PCLUSTERKEYSLOT" if args.len() == 2 => {
            RespValue::Integer(slot_id_for_key(String::from_utf8_lossy(&args[1]).as_ref()) as i64)
        }
        "PCLUSTERHASH" if args.len() == 2 => {
            RespValue::Integer(stable_key_hash(String::from_utf8_lossy(&args[1]).as_ref()) as i64)
        }
        "GET" if args.len() == 2 => bytes_response(execute(Command::StringGet {
            key: string_arg(&args[1]),
        })),
        "MGET" if args.len() >= 2 => {
            let mut values = Vec::new();
            for key in args.iter().skip(1) {
                match execute(Command::StringGet {
                    key: string_arg(key),
                }) {
                    Ok(CommandResponse::Bytes { value }) => values.push(RespValue::Bulk(value)),
                    Ok(_) => return RespValue::Error("ERR invalid mget response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Array(values)
        }
        "GETDEL" if args.len() == 2 => {
            let key = string_arg(&args[1]);
            match execute(Command::StringGet { key: key.clone() }) {
                Ok(CommandResponse::Bytes { value }) => {
                    if value.is_some() {
                        if let Err(err) = execute(Command::CommonDelete { key }) {
                            return RespValue::Error(format!("ERR {err}"));
                        }
                    }
                    RespValue::Bulk(value)
                }
                Ok(_) => RespValue::Error("ERR invalid getdel response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "GETSET" if args.len() == 3 => {
            let key = string_arg(&args[1]);
            match execute(Command::StringGet { key: key.clone() }) {
                Ok(CommandResponse::Bytes { value }) => {
                    if let Err(err) = execute(Command::StringSet {
                        key,
                        value: args[2].clone(),
                    }) {
                        return RespValue::Error(format!("ERR {err}"));
                    }
                    RespValue::Bulk(value)
                }
                Ok(_) => RespValue::Error("ERR invalid getset response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "SET" if args.len() >= 3 => {
            let key = string_arg(&args[1]);
            let value = args[2].clone();
            let options = match parse_set_options(&args[3..]) {
                Ok(options) => options,
                Err(err) => return RespValue::Error(err),
            };
            match execute(Command::StringSetConditional {
                key,
                value,
                ttl_ms: options.ttl_ms,
                condition: options.condition,
                return_old: options.return_old,
            }) {
                Ok(CommandResponse::Bytes { value }) => RespValue::Bulk(value),
                Ok(CommandResponse::Integer { value: 1 }) => {
                    RespValue::SimpleString("OK".to_string())
                }
                Ok(CommandResponse::Integer { value: 0 }) => RespValue::Bulk(None),
                Ok(_) => RespValue::Error("ERR invalid set response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "SETNX" if args.len() == 3 => match execute(Command::StringSetConditional {
            key: string_arg(&args[1]),
            value: args[2].clone(),
            ttl_ms: None,
            condition: StringSetCondition::IfNotExists,
            return_old: false,
        }) {
            Ok(CommandResponse::Integer { value }) => RespValue::Integer(value),
            Ok(CommandResponse::Bytes { value: None }) => RespValue::Integer(0),
            Ok(_) => RespValue::Error("ERR invalid setnx response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "MSET" if args.len() >= 3 && args.len() % 2 == 1 => {
            for pair in args[1..].chunks(2) {
                if let Err(err) = execute(Command::StringSet {
                    key: string_arg(&pair[0]),
                    value: pair[1].clone(),
                }) {
                    return RespValue::Error(format!("ERR {err}"));
                }
            }
            RespValue::SimpleString("OK".to_string())
        }
        "MSETNX" if args.len() >= 3 && args.len() % 2 == 1 => {
            for pair in args[1..].chunks(2) {
                match execute(Command::CommonExists {
                    key: string_arg(&pair[0]),
                }) {
                    Ok(CommandResponse::Integer { value }) if value > 0 => {
                        return RespValue::Integer(0);
                    }
                    Ok(CommandResponse::Integer { .. }) => {}
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            for pair in args[1..].chunks(2) {
                if let Err(err) = execute(Command::StringSet {
                    key: string_arg(&pair[0]),
                    value: pair[1].clone(),
                }) {
                    return RespValue::Error(format!("ERR {err}"));
                }
            }
            RespValue::Integer(1)
        }
        "SETEX" if args.len() == 4 => match parse_u64(&args[2], "seconds") {
            Ok(seconds) => status_ok(execute(Command::StringSetEx {
                key: string_arg(&args[1]),
                value: args[3].clone(),
                ttl_ms: seconds.saturating_mul(1000),
            })),
            Err(err) => RespValue::Error(err),
        },
        "PSETEX" if args.len() == 4 => match parse_u64(&args[2], "milliseconds") {
            Ok(ttl_ms) => status_ok(execute(Command::StringSetEx {
                key: string_arg(&args[1]),
                value: args[3].clone(),
                ttl_ms,
            })),
            Err(err) => RespValue::Error(err),
        },
        "EXISTS" if args.len() >= 2 => {
            let mut count = 0;
            for key in args.iter().skip(1) {
                match execute(Command::CommonExists {
                    key: string_arg(key),
                }) {
                    Ok(CommandResponse::Integer { value }) => count += value,
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Integer(count)
        }
        "DEL" if args.len() >= 2 => {
            let mut removed = 0;
            for key in args.iter().skip(1) {
                let key = string_arg(key);
                match execute(Command::CommonExists { key: key.clone() }) {
                    Ok(CommandResponse::Integer { value }) => {
                        if value > 0 {
                            if let Err(err) = execute(Command::CommonDelete { key }) {
                                return RespValue::Error(format!("ERR {err}"));
                            }
                            removed += 1;
                        }
                    }
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Integer(removed)
        }
        "EXPIRE" if args.len() == 3 => expire_response(&args, 1000, execute),
        "PEXPIRE" if args.len() == 3 => expire_response(&args, 1, execute),
        "TTL" if args.len() == 2 => match execute(Command::CommonTtl {
            key: string_arg(&args[1]),
        }) {
            Ok(CommandResponse::Integer { value }) => RespValue::Integer(value / 1000),
            Ok(_) => RespValue::Error("ERR invalid ttl response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "PTTL" if args.len() == 2 => integer_response(execute(Command::CommonTtl {
            key: string_arg(&args[1]),
        })),
        "STRLEN" if args.len() == 2 => match execute(Command::StringGet {
            key: string_arg(&args[1]),
        }) {
            Ok(CommandResponse::Bytes { value }) => {
                RespValue::Integer(value.map(|value| value.len() as i64).unwrap_or_default())
            }
            Ok(_) => RespValue::Error("ERR invalid strlen response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "APPEND" if args.len() == 3 => {
            let key = string_arg(&args[1]);
            match execute(Command::StringGet { key: key.clone() }) {
                Ok(CommandResponse::Bytes { value }) => {
                    let mut new_value = value.unwrap_or_default();
                    new_value.extend_from_slice(&args[2]);
                    let new_len = new_value.len() as i64;
                    if let Err(err) = execute(Command::StringSet {
                        key,
                        value: new_value,
                    }) {
                        return RespValue::Error(format!("ERR {err}"));
                    }
                    RespValue::Integer(new_len)
                }
                Ok(_) => RespValue::Error("ERR invalid append response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "INCR" if args.len() == 2 => string_increment_response(&args[1], 1, &mut execute),
        "DECR" if args.len() == 2 => string_increment_response(&args[1], -1, &mut execute),
        "INCRBY" if args.len() == 3 => match parse_i64_arg(&args[2], "increment") {
            Ok(increment) => string_increment_response(&args[1], increment, &mut execute),
            Err(err) => RespValue::Error(err),
        },
        "DECRBY" if args.len() == 3 => match parse_i64_arg(&args[2], "decrement") {
            Ok(decrement) => string_increment_response(&args[1], -decrement, &mut execute),
            Err(err) => RespValue::Error(err),
        },
        "HSET" if args.len() >= 4 && args.len() % 2 == 0 => {
            let key = string_arg(&args[1]);
            let mut added = 0;
            for pair in args[2..].chunks(2) {
                let field = string_arg(&pair[0]);
                match execute(Command::HashGet {
                    key: key.clone(),
                    field,
                }) {
                    Ok(CommandResponse::Bytes { value: None }) => added += 1,
                    Ok(CommandResponse::Bytes { value: Some(_) }) => {}
                    Ok(_) => return RespValue::Error("ERR invalid hget response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            let entries = args[2..]
                .chunks(2)
                .map(|pair| (string_arg(&pair[0]), pair[1].clone()))
                .collect();
            match execute(Command::HashMultiSet { key, entries }) {
                Ok(_) => RespValue::Integer(added),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "HMSET" if args.len() >= 4 && args.len() % 2 == 0 => {
            let entries = args[2..]
                .chunks(2)
                .map(|pair| (string_arg(&pair[0]), pair[1].clone()))
                .collect();
            status_ok(execute(Command::HashMultiSet {
                key: string_arg(&args[1]),
                entries,
            }))
        }
        "HGET" if args.len() == 3 => bytes_response(execute(Command::HashGet {
            key: string_arg(&args[1]),
            field: string_arg(&args[2]),
        })),
        "HMGET" if args.len() >= 3 => {
            match execute(Command::HashMultiGet {
                key: string_arg(&args[1]),
                fields: args.iter().skip(2).map(|field| string_arg(field)).collect(),
            }) {
                Ok(CommandResponse::Values { values }) => {
                    RespValue::Array(values.into_iter().map(RespValue::Bulk).collect())
                }
                Ok(_) => RespValue::Error("ERR invalid hmget response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "HINCRBY" if args.len() == 4 => match parse_i64_arg(&args[3], "increment") {
            Ok(increment) => integer_response(execute(Command::HashIncrBy {
                key: string_arg(&args[1]),
                field: string_arg(&args[2]),
                increment,
            })),
            Err(err) => RespValue::Error(err),
        },
        "HGETALL" if args.len() == 2 => match execute(Command::HashGetAll {
            key: string_arg(&args[1]),
        }) {
            Ok(CommandResponse::HashEntries { entries }) => RespValue::Array(
                entries
                    .into_iter()
                    .flat_map(|(field, value)| {
                        vec![
                            RespValue::Bulk(Some(field.into_bytes())),
                            RespValue::Bulk(Some(value)),
                        ]
                    })
                    .collect(),
            ),
            Ok(_) => RespValue::Error("ERR invalid hgetall response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "HLEN" if args.len() == 2 => integer_response(execute(Command::HashLen {
            key: string_arg(&args[1]),
        })),
        "HDEL" if args.len() >= 3 => {
            let key = string_arg(&args[1]);
            let mut removed = 0;
            for field in args.iter().skip(2) {
                let field = string_arg(field);
                match execute(Command::HashGet {
                    key: key.clone(),
                    field: field.clone(),
                }) {
                    Ok(CommandResponse::Bytes { value: Some(_) }) => {
                        if let Err(err) = execute(Command::HashDelete {
                            key: key.clone(),
                            field,
                        }) {
                            return RespValue::Error(format!("ERR {err}"));
                        }
                        removed += 1;
                    }
                    Ok(CommandResponse::Bytes { value: None }) => {}
                    Ok(_) => return RespValue::Error("ERR invalid hget response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Integer(removed)
        }
        "SADD" if args.len() >= 3 => {
            let key = string_arg(&args[1]);
            let mut existing = match execute(Command::SetMembers { key: key.clone() }) {
                Ok(CommandResponse::Members { members }) => {
                    members.into_iter().collect::<HashSet<_>>()
                }
                Ok(_) => return RespValue::Error("ERR invalid smembers response".to_string()),
                Err(err) => return RespValue::Error(format!("ERR {err}")),
            };
            let mut added = 0;
            for member in args.iter().skip(2) {
                if existing.insert(member.clone()) {
                    if let Err(err) = execute(Command::SetAdd {
                        key: key.clone(),
                        member: member.clone(),
                    }) {
                        return RespValue::Error(format!("ERR {err}"));
                    }
                    added += 1;
                }
            }
            RespValue::Integer(added)
        }
        "SMEMBERS" if args.len() == 2 => match execute(Command::SetMembers {
            key: string_arg(&args[1]),
        }) {
            Ok(CommandResponse::Members { members }) => RespValue::Array(
                members
                    .into_iter()
                    .map(|member| RespValue::Bulk(Some(member)))
                    .collect(),
            ),
            Ok(_) => RespValue::Error("ERR invalid smembers response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "SCARD" if args.len() == 2 => match execute(Command::SetMembers {
            key: string_arg(&args[1]),
        }) {
            Ok(CommandResponse::Members { members }) => RespValue::Integer(members.len() as i64),
            Ok(_) => RespValue::Error("ERR invalid smembers response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "SISMEMBER" if args.len() == 3 => match execute(Command::SetMembers {
            key: string_arg(&args[1]),
        }) {
            Ok(CommandResponse::Members { members }) => {
                RespValue::Integer(i64::from(members.iter().any(|member| member == &args[2])))
            }
            Ok(_) => RespValue::Error("ERR invalid smembers response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "SMISMEMBER" if args.len() >= 3 => match execute(Command::SetMembers {
            key: string_arg(&args[1]),
        }) {
            Ok(CommandResponse::Members { members }) => {
                let existing = members.into_iter().collect::<HashSet<_>>();
                RespValue::Array(
                    args.iter()
                        .skip(2)
                        .map(|member| RespValue::Integer(i64::from(existing.contains(member))))
                        .collect(),
                )
            }
            Ok(_) => RespValue::Error("ERR invalid smembers response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "SREM" if args.len() >= 3 => {
            let key = string_arg(&args[1]);
            let mut existing = match execute(Command::SetMembers { key: key.clone() }) {
                Ok(CommandResponse::Members { members }) => {
                    members.into_iter().collect::<HashSet<_>>()
                }
                Ok(_) => return RespValue::Error("ERR invalid smembers response".to_string()),
                Err(err) => return RespValue::Error(format!("ERR {err}")),
            };
            let mut removed = 0;
            for member in args.iter().skip(2) {
                if existing.remove(member) {
                    if let Err(err) = execute(Command::SetRemove {
                        key: key.clone(),
                        member: member.clone(),
                    }) {
                        return RespValue::Error(format!("ERR {err}"));
                    }
                    removed += 1;
                }
            }
            RespValue::Integer(removed)
        }
        "FAPPEND" if args.len() == 4 => match parse_u64(&args[2], "timestamp_ms") {
            Ok(timestamp_ms) => status_ok(execute(Command::FeatureAppend {
                key: string_arg(&args[1]),
                points: vec![FeaturePoint {
                    timestamp_ms,
                    value: args[3].clone(),
                }],
            })),
            Err(err) => RespValue::Error(err),
        },
        "FAPPENDPOLICY" if args.len() == 5 => {
            let timestamp_ms = match parse_u64(&args[2], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let policy = match parse_feature_write_policy(&args[4]) {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            integer_response(execute(Command::FeatureAppendWithPolicy {
                key: string_arg(&args[1]),
                points: vec![FeaturePoint {
                    timestamp_ms,
                    value: args[3].clone(),
                }],
                policy,
            }))
        }
        "FQUERY" if args.len() == 4 || args.len() == 5 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match args.get(4) {
                Some(value) => match parse_usize(value, "count") {
                    Ok(value) => Some(value),
                    Err(err) => return RespValue::Error(err),
                },
                None => None,
            };
            match execute(Command::FeatureQuery {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count,
            }) {
                Ok(CommandResponse::FeaturePoints { points }) => RespValue::Array(
                    points
                        .into_iter()
                        .map(|point| {
                            RespValue::Array(vec![
                                RespValue::Integer(point.timestamp_ms as i64),
                                RespValue::Bulk(Some(point.value)),
                            ])
                        })
                        .collect(),
                ),
                Ok(_) => RespValue::Error("ERR invalid fquery response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "FQUERYFILTER" if args.len() == 8 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match parse_usize(&args[4], "count") {
                Ok(value) => Some(value),
                Err(err) => return RespValue::Error(err),
            };
            let op = match parse_feature_filter_op(&string_arg(&args[6])) {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            match execute(Command::FeatureQueryFiltered {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count,
                filters: vec![FeatureFilter {
                    field: string_arg(&args[5]),
                    op,
                    value: match parse_u64(&args[7], "filter_value") {
                        Ok(value) => value,
                        Err(err) => return RespValue::Error(err),
                    },
                }],
            }) {
                Ok(CommandResponse::FeaturePoints { points }) => RespValue::Array(
                    points
                        .into_iter()
                        .map(|point| {
                            RespValue::Array(vec![
                                RespValue::Integer(point.timestamp_ms as i64),
                                RespValue::Bulk(Some(point.value)),
                            ])
                        })
                        .collect(),
                ),
                Ok(_) => RespValue::Error("ERR invalid fqueryfilter response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "FQUERYFILTERSTR" if args.len() >= 6 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match parse_usize(&args[4], "count") {
                Ok(value) => Some(value),
                Err(err) => return RespValue::Error(err),
            };
            let raw_filters = args
                .iter()
                .skip(5)
                .map(|filter| string_arg(filter))
                .collect::<Vec<_>>();
            let filters = match parse_cpp_feature_filters(raw_filters.iter().map(String::as_str)) {
                Ok(value) => value,
                Err(err) => return RespValue::Error(format!("ERR {err}")),
            };
            match execute(Command::FeatureQueryFiltered {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count,
                filters,
            }) {
                Ok(CommandResponse::FeaturePoints { points }) => RespValue::Array(
                    points
                        .into_iter()
                        .map(|point| {
                            RespValue::Array(vec![
                                RespValue::Integer(point.timestamp_ms as i64),
                                RespValue::Bulk(Some(point.value)),
                            ])
                        })
                        .collect(),
                ),
                Ok(_) => RespValue::Error("ERR invalid fqueryfilterstr response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "FREPLACE" if args.len() == 6 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let timestamp_ms = match parse_u64(&args[4], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            status_ok(execute(Command::FeatureReplace {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                points: vec![FeaturePoint {
                    timestamp_ms,
                    value: args[5].clone(),
                }],
            }))
        }
        "FDEL" if args.len() == 2 => status_ok(execute(Command::FeatureDelete {
            key: string_arg(&args[1]),
        })),
        "FAGG" if args.len() == 5 || args.len() == 6 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match args.get(5) {
                Some(value) => match parse_usize(value, "count") {
                    Ok(value) => Some(value),
                    Err(err) => return RespValue::Error(err),
                },
                None => None,
            };
            match execute(Command::FeatureAggQuery {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                aggregator: string_arg(&args[4]),
                count,
            }) {
                Ok(CommandResponse::Aggregate { value }) => RespValue::Integer(value),
                Ok(_) => RespValue::Error("ERR invalid fagg response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "IPSADD" if args.len() == 4 => match parse_u64(&args[2], "timestamp_ms") {
            Ok(timestamp_ms) => status_ok(execute(Command::IpsAdd {
                key: string_arg(&args[1]),
                timestamp_ms,
                instance: args[3].clone(),
            })),
            Err(err) => RespValue::Error(err),
        },
        "IPSADDOPT" if args.len() == 7 => {
            let timestamp_ms = match parse_u64(&args[2], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let action_type = match parse_u32(&args[4], "action_type") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let table_id = match parse_u64(&args[5], "table_id") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            integer_response(execute(Command::IpsAddWithOptions {
                key: string_arg(&args[1]),
                timestamp_ms,
                instance: args[3].clone(),
                action_type: Some(action_type),
                table_id: Some(table_id),
                request_id: Some(string_arg(&args[6])),
            }))
        }
        "IPSLOAD" if args.len() >= 4 && args.len() % 2 == 0 => {
            let key = string_arg(&args[1]);
            let mut points = Vec::new();
            for pair in args[2..].chunks(2) {
                let timestamp_ms = match parse_u64(&pair[0], "timestamp_ms") {
                    Ok(value) => value,
                    Err(err) => return RespValue::Error(err),
                };
                points.push(FeaturePoint {
                    timestamp_ms,
                    value: pair[1].clone(),
                });
            }
            integer_response(execute(Command::IpsLoad { key, points }))
        }
        "IPSQUERYLAST" if args.len() == 3 => match parse_usize(&args[2], "count") {
            Ok(count) => feature_points_response(execute(Command::IpsQueryLast {
                key: string_arg(&args[1]),
                count,
            })),
            Err(err) => RespValue::Error(err),
        },
        "IPSQUERYRANGE" if args.len() == 4 || args.len() == 5 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match args.get(4) {
                Some(value) => match parse_usize(value, "count") {
                    Ok(value) => Some(value),
                    Err(err) => return RespValue::Error(err),
                },
                None => None,
            };
            feature_points_response(execute(Command::IpsQueryRange {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count,
            }))
        }
        "IPSQUERYRANGEOPT" if args.len() == 7 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match parse_usize(&args[4], "count") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let action_type = match parse_u32(&args[5], "action_type") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let table_id = match parse_u64(&args[6], "table_id") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            feature_points_response(execute(Command::IpsQueryRangeWithOptions {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count: Some(count),
                action_type: Some(action_type),
                table_id: Some(table_id),
            }))
        }
        "IPSSNAPSHOT" if args.len() == 4 || args.len() == 5 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match args.get(4) {
                Some(value) => match parse_usize(value, "count") {
                    Ok(value) => Some(value),
                    Err(err) => return RespValue::Error(err),
                },
                None => None,
            };
            feature_points_response(execute(Command::IpsSnapshot {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count,
            }))
        }
        "IPSSNAPSHOTREPORT" if args.len() == 4 || args.len() == 5 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match args.get(4) {
                Some(value) => match parse_usize(value, "count") {
                    Ok(value) => Some(value),
                    Err(err) => return RespValue::Error(err),
                },
                None => None,
            };
            ips_snapshot_report_response(execute(Command::IpsSnapshotReport {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count,
            }))
        }
        "IPSSTAT" if args.len() == 4 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            ips_stats_response(execute(Command::IpsStat {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
            }))
        }
        "IPSFILTER" if args.len() == 7 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match parse_usize(&args[4], "count") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let action_type = match parse_u32(&args[5], "action_type") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let table_id = match parse_u64(&args[6], "table_id") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            feature_points_response(execute(Command::IpsFilter {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count: Some(count),
                action_type: Some(action_type),
                table_id: Some(table_id),
            }))
        }
        "IPSBATCHQUERYLAST" if args.len() >= 4 => {
            let count = match parse_usize(&args[1], "count") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let keys = args[2..].iter().map(|arg| string_arg(arg)).collect();
            match execute(Command::IpsBatchQueryLast { keys, count }) {
                Ok(CommandResponse::FeaturePointGroups { groups }) => RespValue::Array(
                    groups
                        .into_iter()
                        .map(|(key, points)| {
                            RespValue::Array(vec![
                                RespValue::Bulk(Some(key.into_bytes())),
                                feature_points_value(points),
                            ])
                        })
                        .collect(),
                ),
                Ok(_) => RespValue::Error("ERR invalid ips batch response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "IPSREMOVE" if args.len() == 3 => match parse_u64(&args[2], "timestamp_ms") {
            Ok(timestamp_ms) => integer_response(execute(Command::IpsRemove {
                key: string_arg(&args[1]),
                timestamp_ms,
            })),
            Err(err) => RespValue::Error(err),
        },
        "IPSDEL" if args.len() == 2 => integer_response(execute(Command::IpsDelete {
            key: string_arg(&args[1]),
        })),
        "IPSCOUNT" if args.len() == 4 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            integer_response(execute(Command::IpsCount {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
            }))
        }
        "RISKINCR" if args.len() == 4 => {
            let timestamp_ms = match parse_u64(&args[2], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let amount = match parse_i64_arg(&args[3], "amount") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            status_ok(execute(Command::RiskIncrement {
                key: string_arg(&args[1]),
                timestamp_ms,
                amount,
            }))
        }
        "RISKINCROPT" if args.len() == 6 => {
            let timestamp_ms = match parse_u64(&args[2], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let amount = match parse_i64_arg(&args[3], "amount") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let precision_ms = match parse_u64(&args[4], "precision_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let ttl_ms = match parse_u64(&args[5], "ttl_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            status_ok(execute(Command::RiskIncrementWithOptions {
                key: string_arg(&args[1]),
                timestamp_ms,
                amount,
                precision_ms: Some(precision_ms),
                ttl_ms: Some(ttl_ms),
            }))
        }
        "RISKCHANGE" | "HCHANGE" if args.len() == 4 || args.len() == 6 => {
            let timestamp_ms = match parse_u64(&args[2], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let (precision_ms, ttl_ms) = if args.len() == 6 {
                let precision_ms = match parse_u64(&args[4], "precision_ms") {
                    Ok(value) => value,
                    Err(err) => return RespValue::Error(err),
                };
                let ttl_ms = match parse_u64(&args[5], "ttl_ms") {
                    Ok(value) => value,
                    Err(err) => return RespValue::Error(err),
                };
                (Some(precision_ms), Some(ttl_ms))
            } else {
                (None, None)
            };
            let key = if command == "HCHANGE" {
                risk_family_key_for_resp(RiskFamily::H, &string_arg(&args[1]))
            } else {
                string_arg(&args[1])
            };
            status_ok(execute(Command::RiskChangeAdd {
                key,
                timestamp_ms,
                value: args[3].clone(),
                precision_ms,
                ttl_ms,
            }))
        }
        "RISKCOUNT" if args.len() == 4 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            integer_response(execute(Command::RiskCount {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
            }))
        }
        "RISKQUERY" if args.len() == 5 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            integer_response(execute(Command::RiskQuery {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                aggregator: string_arg(&args[4]),
            }))
        }
        "RISKDETAIL" if args.len() == 4 || args.len() == 5 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let count = match args.get(4) {
                Some(value) => match parse_usize(value, "count") {
                    Ok(value) => Some(value),
                    Err(err) => return RespValue::Error(err),
                },
                None => None,
            };
            feature_points_response(execute(Command::RiskDetail {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count,
            }))
        }
        "RISKHSET" | "CPCSET" | "FOLSET" if args.len() == 4 => {
            let timestamp_ms = match parse_u64(&args[2], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let amount = match parse_i64_arg(&args[3], "amount") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            status_ok(execute(Command::RiskSet {
                family: risk_family_for_command(&command),
                key: string_arg(&args[1]),
                timestamp_ms,
                amount,
            }))
        }
        "FOLSET" if args.len() == 6 => {
            let occur_time_ms = match parse_u64(&args[3], "occur_time_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let ttl_ms = match parse_u64(&args[4], "ttl_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let fol_type = match upper(&args[5]).as_str() {
                "FIRST" => RiskFolType::First,
                "LAST" => RiskFolType::Last,
                value => return RespValue::Error(format!("ERR unsupported fol_type: {value}")),
            };
            status_ok(execute(Command::RiskFolSet {
                key: string_arg(&args[1]),
                value: args[2].clone(),
                occur_time_ms,
                ttl_ms,
                fol_type,
            }))
        }
        "FOLQUERY" if args.len() == 2 => bytes_response(execute(Command::RiskFolQuery {
            key: string_arg(&args[1]),
        })),
        "HQUERY" | "CPCQUERY" | "FOLQUERY" if args.len() == 5 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            integer_response(execute(Command::RiskFamilyQuery {
                family: risk_family_for_command(&command),
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                aggregator: string_arg(&args[4]),
            }))
        }
        "HSETANDGET" | "CPCSETANDGET" | "FOLSETANDGET" if args.len() == 7 => {
            let timestamp_ms = match parse_u64(&args[2], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let amount = match parse_i64_arg(&args[3], "amount") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let start_ms = match parse_u64(&args[4], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[5], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            integer_response(execute(Command::RiskSetAndGet {
                family: risk_family_for_command(&command),
                key: string_arg(&args[1]),
                timestamp_ms,
                amount,
                start_ms,
                end_ms,
                aggregator: string_arg(&args[6]),
            }))
        }
        "RISKMANAGER" if args.len() == 2 => hash_entries_response(execute(Command::RiskManager {
            key: string_arg(&args[1]),
        })),
        "RISKDEBUG" if args.len() == 4 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            hash_entries_response(execute(Command::RiskDebug {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
            }))
        }
        _ => RespValue::Error(format!("ERR unsupported command or arity: {command}")),
    }
}

fn risk_family_for_command(command: &str) -> RiskFamily {
    if command.starts_with("CPC") {
        RiskFamily::Cpc
    } else if command.starts_with("FOL") {
        RiskFamily::Fol
    } else {
        RiskFamily::H
    }
}

fn risk_family_key_for_resp(family: RiskFamily, key: &str) -> String {
    let family_name = match family {
        RiskFamily::H => "h",
        RiskFamily::Cpc => "cpc",
        RiskFamily::Fol => "fol",
    };
    format!("risk:{family_name}:{key}")
}

fn redis_config_response(args: &[Vec<u8>], state: &mut RedisCommandState) -> RespValue {
    match upper(&args[1]).as_str() {
        "GET" if args.len() == 3 => {
            let key = string_arg(&args[2]);
            match state.config.get(&key) {
                Some(value) => RespValue::Array(vec![
                    RespValue::Bulk(Some(key.into_bytes())),
                    RespValue::Bulk(Some(value.clone().into_bytes())),
                ]),
                None => RespValue::Array(Vec::new()),
            }
        }
        "SET" if args.len() == 4 => {
            state
                .config
                .insert(string_arg(&args[2]), string_arg(&args[3]));
            RespValue::SimpleString("OK".to_string())
        }
        "REWRITE" if args.len() == 2 => RespValue::SimpleString("OK".to_string()),
        _ => RespValue::Error("ERR syntax error".to_string()),
    }
}

fn redis_partition_response(
    args: &[Vec<u8>],
    shard_id: ShardId,
    state: &mut RedisCommandState,
) -> RespValue {
    match upper(&args[1]).as_str() {
        "LOAD" if args.len() >= 3 => {
            let loaded = parse_u64(&args[2], "partition_id").unwrap_or(shard_id);
            state.loaded_shard_id = Some(loaded);
            RespValue::SimpleString("OK".to_string())
        }
        "UNLOAD" if args.len() >= 3 => {
            state.loaded_shard_id = None;
            RespValue::SimpleString("OK".to_string())
        }
        "INFO" if args.len() == 2 || args.len() == 3 => RespValue::Bulk(Some(
            format!(
                "partition_id:{}\r\npartition_loading_stats:{}\r\n",
                state.loaded_shard_id.unwrap_or(shard_id),
                if state.loaded_shard_id.is_some() {
                    "loaded"
                } else {
                    "not_exist"
                }
            )
            .into_bytes(),
        )),
        _ => RespValue::Error("ERR syntax error".to_string()),
    }
}

fn redis_info(section: &str, shard_id: ShardId, state: &RedisCommandState) -> String {
    let section = section.to_ascii_lowercase();
    let all = section == "all";
    let default = section == "default";
    let mut parts = Vec::new();
    if all || default || section == "server" {
        parts.push(
            "# Server\r\nredis_version:temporalstore-rust\r\nredis_mode:temporalstore\r\narch_bits:64\r\nmultiplexing_api:std-tcp\r\n".to_string(),
        );
    }
    if all || default || section == "clients" {
        parts.push("# Clients\r\nconnected_clients:1\r\nblocked_clients:0\r\n".to_string());
    }
    if all || default || section == "memory" {
        let maxmemory = state
            .config
            .get("maxmemory")
            .map(String::as_str)
            .unwrap_or("0");
        let policy = state
            .config
            .get("maxmemory-policy")
            .map(String::as_str)
            .unwrap_or("noeviction");
        parts.push(format!(
            "# Memory\r\nused_memory:0\r\nmaxmemory:{maxmemory}\r\nmaxmemory_policy:{policy}\r\n"
        ));
    }
    if all || default || section == "stats" {
        let loading = if state.loaded_shard_id.is_some() {
            "loaded"
        } else {
            "not_exist"
        };
        parts.push(format!(
            "# Stats\r\npartition_loading_stats:{loading}\r\ntotal_commands_processed:0\r\n"
        ));
    }
    if all || default || section == "replication" {
        let mut replication = "# Replication\r\n".to_string();
        if let Some((host, port)) = &state.master {
            replication.push_str("role:slave\r\n");
            replication.push_str(&format!(
                "master_host:{host}\r\nmaster_port:{port}\r\nmaster_link_status:up\r\n"
            ));
        } else {
            replication.push_str("role:master\r\n");
        }
        replication.push_str("connected_slaves:0\r\n");
        parts.push(replication);
    }
    if all || default || section == "cluster" {
        parts.push(format!(
            "# Cluster\r\ncluster_enabled:0\r\nloaded_shard_id:{}\r\n",
            state.loaded_shard_id.unwrap_or(shard_id)
        ));
    }
    if parts.is_empty() {
        return String::new();
    }
    parts.join("\r\n")
}

pub fn read_command(reader: &mut impl BufRead) -> io::Result<Option<Vec<Vec<u8>>>> {
    let mut first = Vec::new();
    let bytes = reader.read_until(b'\n', &mut first)?;
    if bytes == 0 {
        return Ok(None);
    }
    trim_crlf(&mut first);
    if first.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if first[0] != b'*' {
        return Ok(Some(split_inline(&first)));
    }
    let count = parse_prefixed_number(&first, b'*')?;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let mut len_line = Vec::new();
        reader.read_until(b'\n', &mut len_line)?;
        trim_crlf(&mut len_line);
        let len = parse_prefixed_number(&len_line, b'$')?;
        let mut value = vec![0; len + 2];
        reader.read_exact(&mut value)?;
        if &value[len..] != b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad bulk string terminator",
            ));
        }
        value.truncate(len);
        args.push(value);
    }
    Ok(Some(args))
}

#[derive(Debug, Clone, Copy)]
struct SetOptions {
    ttl_ms: Option<u64>,
    condition: StringSetCondition,
    return_old: bool,
}

fn parse_set_options(args: &[Vec<u8>]) -> Result<SetOptions, String> {
    let mut options = SetOptions {
        ttl_ms: None,
        condition: StringSetCondition::Always,
        return_old: false,
    };
    let mut index = 0;
    while index < args.len() {
        match upper(&args[index]).as_str() {
            "EX" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                if options
                    .ttl_ms
                    .replace(parse_u64(value, "seconds")?.saturating_mul(1000))
                    .is_some()
                {
                    return Err("ERR syntax error".to_string());
                }
                index += 2;
            }
            "PX" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                if options
                    .ttl_ms
                    .replace(parse_u64(value, "milliseconds")?)
                    .is_some()
                {
                    return Err("ERR syntax error".to_string());
                }
                index += 2;
            }
            "NX" => {
                if options.condition != StringSetCondition::Always {
                    return Err("ERR syntax error".to_string());
                }
                options.condition = StringSetCondition::IfNotExists;
                index += 1;
            }
            "XX" => {
                if options.condition != StringSetCondition::Always {
                    return Err("ERR syntax error".to_string());
                }
                options.condition = StringSetCondition::IfExists;
                index += 1;
            }
            "GET" => {
                if options.return_old {
                    return Err("ERR syntax error".to_string());
                }
                options.return_old = true;
                index += 1;
            }
            _ => return Err("ERR syntax error".to_string()),
        }
    }
    Ok(options)
}

fn expire_response(
    args: &[Vec<u8>],
    factor: u64,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    match parse_u64(&args[2], "ttl") {
        Ok(ttl) => match execute(Command::CommonExpire {
            key: string_arg(&args[1]),
            ttl_ms: ttl.saturating_mul(factor),
        }) {
            Ok(_) => RespValue::Integer(1),
            Err(err) if err.contains("not_found") || err.contains("key not found") => {
                RespValue::Integer(0)
            }
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        Err(err) => RespValue::Error(err),
    }
}

fn string_increment_response(
    key: &[u8],
    increment: i64,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(key);
    let current = match execute(Command::StringGet { key: key.clone() }) {
        Ok(CommandResponse::Bytes { value: None }) => 0,
        Ok(CommandResponse::Bytes { value: Some(value) }) => match parse_i64_arg(&value, "value") {
            Ok(value) => value,
            Err(_) => return RespValue::Error("ERR value is not an integer".to_string()),
        },
        Ok(_) => return RespValue::Error("ERR invalid incr response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    };
    let Some(next) = current.checked_add(increment) else {
        return RespValue::Error("ERR increment or decrement would overflow".to_string());
    };
    if let Err(err) = execute(Command::StringSet {
        key,
        value: next.to_string().into_bytes(),
    }) {
        return RespValue::Error(format!("ERR {err}"));
    }
    RespValue::Integer(next)
}

fn bytes_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::Bytes { value }) => RespValue::Bulk(value),
        Ok(_) => RespValue::Error("ERR invalid bulk response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn integer_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::Integer { value }) => RespValue::Integer(value),
        Ok(_) => RespValue::Error("ERR invalid integer response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn status_ok(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(_) => RespValue::SimpleString("OK".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn feature_points_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::FeaturePoints { points }) => feature_points_value(points),
        Ok(_) => RespValue::Error("ERR invalid feature points response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn hash_entries_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::HashEntries { entries }) => RespValue::Array(
            entries
                .into_iter()
                .flat_map(|(field, value)| {
                    [
                        RespValue::Bulk(Some(field.into_bytes())),
                        RespValue::Bulk(Some(value)),
                    ]
                })
                .collect(),
        ),
        Ok(_) => RespValue::Error("ERR invalid hash entries response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn ips_stats_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::IpsStats { stats }) => RespValue::Array(vec![
            RespValue::Integer(stats.total as i64),
            optional_u64_value(stats.first_timestamp_ms),
            optional_u64_value(stats.last_timestamp_ms),
            count_pairs_u32_value(stats.action_type_counts),
            count_pairs_u64_value(stats.table_id_counts),
        ]),
        Ok(_) => RespValue::Error("ERR invalid ips stats response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn ips_snapshot_report_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::IpsSnapshotReport { report }) => RespValue::Array(vec![
            RespValue::Bulk(Some(report.key.into_bytes())),
            RespValue::Integer(report.start_ms as i64),
            RespValue::Integer(report.end_ms as i64),
            optional_usize_value(report.requested_count),
            RespValue::Integer(report.returned_count as i64),
            RespValue::Integer(report.total_in_range as i64),
            optional_u64_value(report.first_timestamp_ms),
            optional_u64_value(report.last_timestamp_ms),
            count_pairs_u32_value(report.action_type_counts),
            count_pairs_u64_value(report.table_id_counts),
            RespValue::Integer(report.unique_page_ref_count as i64),
            RespValue::Integer(report.packed_timestamped_page_count as i64),
            RespValue::Array(
                report
                    .page_segment_ids
                    .into_iter()
                    .map(|segment_id| RespValue::Integer(segment_id as i64))
                    .collect(),
            ),
        ]),
        Ok(_) => RespValue::Error("ERR invalid ips snapshot report response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn optional_u64_value(value: Option<u64>) -> RespValue {
    match value {
        Some(value) => RespValue::Integer(value as i64),
        None => RespValue::Bulk(None),
    }
}

fn optional_usize_value(value: Option<usize>) -> RespValue {
    match value {
        Some(value) => RespValue::Integer(value as i64),
        None => RespValue::Bulk(None),
    }
}

fn count_pairs_u32_value(counts: Vec<(u32, u64)>) -> RespValue {
    RespValue::Array(
        counts
            .into_iter()
            .map(|(key, count)| {
                RespValue::Array(vec![
                    RespValue::Integer(key as i64),
                    RespValue::Integer(count as i64),
                ])
            })
            .collect(),
    )
}

fn count_pairs_u64_value(counts: Vec<(u64, u64)>) -> RespValue {
    RespValue::Array(
        counts
            .into_iter()
            .map(|(key, count)| {
                RespValue::Array(vec![
                    RespValue::Integer(key as i64),
                    RespValue::Integer(count as i64),
                ])
            })
            .collect(),
    )
}

fn feature_points_value(points: Vec<FeaturePoint>) -> RespValue {
    RespValue::Array(
        points
            .into_iter()
            .map(|point| {
                RespValue::Array(vec![
                    RespValue::Integer(point.timestamp_ms as i64),
                    RespValue::Bulk(Some(point.value)),
                ])
            })
            .collect(),
    )
}

fn parse_prefixed_number(line: &[u8], prefix: u8) -> io::Result<usize> {
    if !line.starts_with(&[prefix]) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected RESP prefix",
        ));
    }
    std::str::from_utf8(&line[1..])
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad RESP length"))
}

fn parse_u64(value: &[u8], name: &str) -> Result<u64, String> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| format!("ERR {name} must be an unsigned integer"))
}

fn parse_usize(value: &[u8], name: &str) -> Result<usize, String> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| format!("ERR {name} must be an unsigned integer"))
}

fn parse_u32(value: &[u8], name: &str) -> Result<u32, String> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| format!("ERR {name} must be an unsigned integer"))
}

fn parse_i64_arg(value: &[u8], name: &str) -> Result<i64, String> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| format!("ERR {name} must be an integer"))
}

fn parse_feature_write_policy(value: &[u8]) -> Result<FeatureWritePolicy, String> {
    match upper(value).as_str() {
        "UPSERT" => Ok(FeatureWritePolicy::Upsert),
        "NX" | "INSERT_IF_ABSENT" => Ok(FeatureWritePolicy::InsertIfAbsent),
        "XX" | "REPLACE_EXISTING" => Ok(FeatureWritePolicy::ReplaceExisting),
        _ => Err("ERR policy must be UPSERT, NX, or XX".to_string()),
    }
}

fn parse_feature_filter_op(value: &str) -> Result<FeatureFilterOp, String> {
    match value.to_ascii_uppercase().as_str() {
        "=" | "==" | "EQ" => Ok(FeatureFilterOp::Equal),
        "!=" | "<>" | "NE" => Ok(FeatureFilterOp::NotEqual),
        ">" | "GT" => Ok(FeatureFilterOp::GreaterThan),
        ">=" | "GE" | "GTE" => Ok(FeatureFilterOp::GreaterOrEqual),
        "<" | "LT" => Ok(FeatureFilterOp::LessThan),
        "<=" | "LE" | "LTE" => Ok(FeatureFilterOp::LessOrEqual),
        _ => Err("ERR filter op must be =, !=, >, >=, <, or <=".to_string()),
    }
}

fn split_inline(line: &[u8]) -> Vec<Vec<u8>> {
    String::from_utf8_lossy(line)
        .split_whitespace()
        .map(|part| part.as_bytes().to_vec())
        .collect()
}

fn string_arg(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_string()
}

fn upper(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_ascii_uppercase()
}

fn trim_crlf(value: &mut Vec<u8>) {
    while value
        .last()
        .is_some_and(|byte| *byte == b'\n' || *byte == b'\r')
    {
        value.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TemporalEngine;
    use crate::types::SequenceFeatureRow;

    #[test]
    fn resp_parser_reads_array_command() {
        let mut input = BufReader::new(&b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"[..]);
        assert_eq!(
            read_command(&mut input).unwrap(),
            Some(vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()])
        );
    }

    #[test]
    fn redis_string_hash_set_and_feature_commands_use_engine() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let run = |args: Vec<&str>| {
            execute_redis_command(
                args.into_iter()
                    .map(|arg| arg.as_bytes().to_vec())
                    .collect(),
                1,
                |command| {
                    let response = engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };

        assert_eq!(
            run(vec!["SET", "k", "v"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["SET", "k", "ignored", "NX"]),
            RespValue::Bulk(None)
        );
        assert_eq!(
            run(vec!["SET", "k", "v2", "XX", "GET"]),
            RespValue::Bulk(Some(b"v".to_vec()))
        );
        assert_eq!(
            run(vec!["SET", "new", "v", "NX", "PX", "1000"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(run(vec!["GET", "k"]), RespValue::Bulk(Some(b"v2".to_vec())));
        assert_eq!(run(vec!["SETNX", "k", "nope"]), RespValue::Integer(0));
        assert_eq!(run(vec!["SETNX", "nx", "yes"]), RespValue::Integer(1));
        assert_eq!(
            run(vec!["GETSET", "nx", "after"]),
            RespValue::Bulk(Some(b"yes".to_vec()))
        );
        assert_eq!(
            run(vec!["MSETNX", "nx", "blocked", "msetnx-new", "v"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(vec!["MSETNX", "msetnx-a", "a", "msetnx-b", "b"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(vec!["APPEND", "append", "hello"]),
            RespValue::Integer(5)
        );
        assert_eq!(
            run(vec!["APPEND", "append", "-world"]),
            RespValue::Integer(11)
        );
        assert_eq!(run(vec!["STRLEN", "append"]), RespValue::Integer(11));
        assert_eq!(run(vec!["INCR", "counter"]), RespValue::Integer(1));
        assert_eq!(run(vec!["INCRBY", "counter", "4"]), RespValue::Integer(5));
        assert_eq!(run(vec!["DECR", "counter"]), RespValue::Integer(4));
        assert_eq!(run(vec!["DECRBY", "counter", "2"]), RespValue::Integer(2));
        assert_eq!(
            run(vec!["SET", "not-int", "abc"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["INCR", "not-int"]),
            RespValue::Error("ERR value is not an integer".to_string())
        );
        assert_eq!(
            run(vec!["MSET", "k1", "v1", "k2", "v2"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["MGET", "k1", "missing", "k2"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"v1".to_vec())),
                RespValue::Bulk(None),
                RespValue::Bulk(Some(b"v2".to_vec())),
            ])
        );
        assert_eq!(
            run(vec!["EXISTS", "k", "missing", "k1"]),
            RespValue::Integer(2)
        );
        assert_eq!(run(vec!["EXPIRE", "missing", "10"]), RespValue::Integer(0));
        assert_eq!(run(vec!["EXPIRE", "k", "10"]), RespValue::Integer(1));
        assert_eq!(
            run(vec!["GETDEL", "k1"]),
            RespValue::Bulk(Some(b"v1".to_vec()))
        );
        assert_eq!(run(vec!["GET", "k1"]), RespValue::Bulk(None));
        assert_eq!(
            run(vec!["DEL", "k", "missing", "k2"]),
            RespValue::Integer(2)
        );
        assert_eq!(run(vec!["HSET", "h", "f", "x"]), RespValue::Integer(1));
        assert_eq!(
            run(vec!["HGET", "h", "f"]),
            RespValue::Bulk(Some(b"x".to_vec()))
        );
        assert_eq!(
            run(vec!["HSET", "h", "f", "x2", "f2", "y"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(vec!["HMGET", "h", "f", "f2", "missing"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"x2".to_vec())),
                RespValue::Bulk(Some(b"y".to_vec())),
                RespValue::Bulk(None),
            ])
        );
        assert_eq!(
            run(vec!["HDEL", "h", "missing", "f2"]),
            RespValue::Integer(1)
        );
        assert_eq!(run(vec!["HLEN", "h"]), RespValue::Integer(1));
        assert_eq!(run(vec!["HINCRBY", "h", "n", "3"]), RespValue::Integer(3));
        assert_eq!(run(vec!["HINCRBY", "h", "n", "-1"]), RespValue::Integer(2));
        assert_eq!(run(vec!["HSET", "h", "bad", "abc"]), RespValue::Integer(1));
        assert_eq!(
            run(vec!["HINCRBY", "h", "bad", "1"]),
            RespValue::Error("ERR hash value is not an integer".to_string())
        );
        assert_eq!(run(vec!["SADD", "s", "m", "m2"]), RespValue::Integer(2));
        assert_eq!(run(vec!["SADD", "s", "m", "m3"]), RespValue::Integer(1));
        assert_eq!(run(vec!["SCARD", "s"]), RespValue::Integer(3));
        assert_eq!(run(vec!["SISMEMBER", "s", "m2"]), RespValue::Integer(1));
        assert_eq!(
            run(vec!["SMISMEMBER", "s", "m", "missing", "m3"]),
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::Integer(0),
                RespValue::Integer(1),
            ])
        );
        assert_eq!(
            run(vec!["SREM", "s", "missing", "m2"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(vec!["SMEMBERS", "s"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"m".to_vec())),
                RespValue::Bulk(Some(b"m3".to_vec())),
            ])
        );
        assert_eq!(
            run(vec!["FAPPEND", "feature", "10", "2"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["FAPPEND", "feature", "20", "3"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["FAGG", "feature", "0", "30", "sum"]),
            RespValue::Integer(5)
        );
        assert_eq!(
            run(vec!["FAPPENDPOLICY", "feature", "20", "ignored", "NX"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(vec!["FAPPENDPOLICY", "feature", "20", "9", "XX"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(vec!["FAGG", "feature", "0", "30", "max"]),
            RespValue::Integer(9)
        );
        assert_eq!(
            run(vec!["IPSADD", "ips-a", "10", "a10"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["IPSADD", "ips-a", "20", "a20"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["IPSADD", "ips-b", "15", "b15"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["IPSQUERYRANGE", "ips-a", "0", "30", "1"]),
            RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(10),
                RespValue::Bulk(Some(b"a10".to_vec())),
            ])])
        );
        assert_eq!(
            run(vec!["IPSQUERYLAST", "ips-a", "1"]),
            RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(20),
                RespValue::Bulk(Some(b"a20".to_vec())),
            ])])
        );
        assert_eq!(
            run(vec!["IPSBATCHQUERYLAST", "1", "ips-a", "ips-b"]),
            RespValue::Array(vec![
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"ips-a".to_vec())),
                    RespValue::Array(vec![RespValue::Array(vec![
                        RespValue::Integer(20),
                        RespValue::Bulk(Some(b"a20".to_vec())),
                    ])]),
                ]),
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"ips-b".to_vec())),
                    RespValue::Array(vec![RespValue::Array(vec![
                        RespValue::Integer(15),
                        RespValue::Bulk(Some(b"b15".to_vec())),
                    ])]),
                ]),
            ])
        );
        assert_eq!(
            run(vec!["IPSCOUNT", "ips-a", "0", "30"]),
            RespValue::Integer(2)
        );
        assert_eq!(run(vec!["IPSREMOVE", "ips-a", "10"]), RespValue::Integer(1));
        assert_eq!(run(vec!["IPSDEL", "ips-a"]), RespValue::Integer(1));
        assert_eq!(
            run(vec![
                "IPSADDOPT",
                "ips-opt",
                "10",
                "x10",
                "7",
                "99",
                "req-1"
            ]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(vec![
                "IPSADDOPT",
                "ips-opt",
                "20",
                "x20",
                "8",
                "99",
                "req-2"
            ]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(vec![
                "IPSADDOPT",
                "ips-opt",
                "30",
                "dup",
                "7",
                "99",
                "req-1"
            ]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(vec![
                "IPSQUERYRANGEOPT",
                "ips-opt",
                "0",
                "40",
                "10",
                "7",
                "99",
            ]),
            RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(10),
                RespValue::Bulk(Some(b"x10".to_vec())),
            ])])
        );
        assert_eq!(
            run(vec!["IPSLOAD", "ips-load", "10", "l10", "20", "l20"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(vec!["IPSSNAPSHOT", "ips-load", "0", "30"]),
            RespValue::Array(vec![
                RespValue::Array(vec![
                    RespValue::Integer(10),
                    RespValue::Bulk(Some(b"l10".to_vec())),
                ]),
                RespValue::Array(vec![
                    RespValue::Integer(20),
                    RespValue::Bulk(Some(b"l20".to_vec())),
                ]),
            ])
        );
        assert_eq!(
            run(vec!["IPSFILTER", "ips-opt", "0", "40", "10", "7", "99"]),
            RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(10),
                RespValue::Bulk(Some(b"x10".to_vec())),
            ])])
        );
        assert_eq!(
            run(vec!["IPSSTAT", "ips-opt", "0", "40"]),
            RespValue::Array(vec![
                RespValue::Integer(2),
                RespValue::Integer(10),
                RespValue::Integer(20),
                RespValue::Array(vec![
                    RespValue::Array(vec![RespValue::Integer(7), RespValue::Integer(1)]),
                    RespValue::Array(vec![RespValue::Integer(8), RespValue::Integer(1)]),
                ]),
                RespValue::Array(vec![RespValue::Array(vec![
                    RespValue::Integer(99),
                    RespValue::Integer(2),
                ])]),
            ])
        );
        assert_eq!(
            run(vec!["IPSSNAPSHOTREPORT", "ips-opt", "0", "40", "1"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"ips-opt".to_vec())),
                RespValue::Integer(0),
                RespValue::Integer(40),
                RespValue::Integer(1),
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(10),
                RespValue::Integer(20),
                RespValue::Array(vec![
                    RespValue::Array(vec![RespValue::Integer(7), RespValue::Integer(1)]),
                    RespValue::Array(vec![RespValue::Integer(8), RespValue::Integer(1)]),
                ]),
                RespValue::Array(vec![RespValue::Array(vec![
                    RespValue::Integer(99),
                    RespValue::Integer(2),
                ])]),
                RespValue::Integer(2),
                RespValue::Integer(2),
                RespValue::Array(vec![RespValue::Integer(0)]),
            ])
        );

        assert_eq!(
            run(vec!["RISKINCR", "risk", "10", "5"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["RISKINCR", "risk", "20", "-2"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["RISKINCR", "risk", "30", "7"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["RISKCOUNT", "risk", "0", "40"]),
            RespValue::Integer(10)
        );
        assert_eq!(
            run(vec!["RISKQUERY", "risk", "0", "40", "events"]),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(vec!["RISKQUERY", "risk", "0", "40", "last"]),
            RespValue::Integer(7)
        );
        assert_eq!(
            run(vec!["RISKDETAIL", "risk", "15", "40", "2"]),
            RespValue::Array(vec![
                RespValue::Array(vec![
                    RespValue::Integer(20),
                    RespValue::Bulk(Some(b"-2".to_vec())),
                ]),
                RespValue::Array(vec![
                    RespValue::Integer(30),
                    RespValue::Bulk(Some(b"7".to_vec())),
                ]),
            ])
        );
        assert_eq!(
            run(vec![
                "RISKINCROPT",
                "risk-bucket",
                "1234",
                "3",
                "1000",
                "60000",
            ]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec![
                "RISKINCROPT",
                "risk-bucket",
                "1999",
                "4",
                "1000",
                "60000",
            ]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["RISKDETAIL", "risk-bucket", "0", "2000", "10"]),
            RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(1000),
                RespValue::Bulk(Some(b"7".to_vec())),
            ])])
        );
        assert_eq!(
            run(vec!["RISKCHANGE", "risk-change", "10", "device-a"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["RISKCHANGE", "risk-change", "20", "device-a"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec![
                "RISKCHANGE",
                "risk-change",
                "30",
                "device-b",
                "10",
                "60000",
            ]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["RISKQUERY", "risk-change", "0", "40", "change"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(vec!["RISKHSET", "risk-cpp", "10", "5"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["HCHANGE", "risk-cpp", "10", "buyer-a"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["HCHANGE", "risk-cpp", "20", "buyer-a"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["HCHANGE", "risk-cpp", "30", "buyer-b"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["HQUERY", "risk-cpp", "0", "40", "change"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(vec!["HSETANDGET", "risk-cpp", "20", "7", "0", "30", "sum"]),
            RespValue::Integer(12)
        );
        assert_eq!(
            run(vec!["CPCSET", "risk-cpp", "10", "3"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec![
                "CPCSETANDGET",
                "risk-cpp",
                "20",
                "4",
                "0",
                "30",
                "sum"
            ]),
            RespValue::Integer(7)
        );
        assert_eq!(
            run(vec!["FOLSET", "risk-cpp", "10", "11"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["FOLQUERY", "risk-cpp", "0", "30", "sum"]),
            RespValue::Integer(11)
        );
        assert_eq!(
            run(vec!["RISKMANAGER", "risk-cpp"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"h_events".to_vec())),
                RespValue::Bulk(Some(b"2".to_vec())),
                RespValue::Bulk(Some(b"h_sum".to_vec())),
                RespValue::Bulk(Some(b"12".to_vec())),
                RespValue::Bulk(Some(b"cpc_events".to_vec())),
                RespValue::Bulk(Some(b"2".to_vec())),
                RespValue::Bulk(Some(b"cpc_sum".to_vec())),
                RespValue::Bulk(Some(b"7".to_vec())),
                RespValue::Bulk(Some(b"fol_events".to_vec())),
                RespValue::Bulk(Some(b"1".to_vec())),
                RespValue::Bulk(Some(b"fol_sum".to_vec())),
                RespValue::Bulk(Some(b"11".to_vec())),
            ])
        );
        let debug = run(vec!["RISKDEBUG", "risk-cpp", "0", "15"]);
        let RespValue::Array(debug_entries) = debug else {
            panic!("RISKDEBUG should return array");
        };
        assert!(debug_entries.windows(2).any(|pair| pair
            == [
                RespValue::Bulk(Some(b"key".to_vec())),
                RespValue::Bulk(Some(b"risk-cpp".to_vec()))
            ]));
        assert!(debug_entries.windows(2).any(|pair| pair
            == [
                RespValue::Bulk(Some(b"h_window_events".to_vec())),
                RespValue::Bulk(Some(b"1".to_vec()))
            ]));
        assert!(debug_entries.windows(2).any(|pair| pair
            == [
                RespValue::Bulk(Some(b"cpc_window_sum".to_vec())),
                RespValue::Bulk(Some(b"3".to_vec()))
            ]));
        assert_eq!(
            run(vec![
                "FOLSET",
                "risk-fol-str",
                "middle",
                "20",
                "60000",
                "FIRST"
            ]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec![
                "FOLSET",
                "risk-fol-str",
                "first",
                "10",
                "60000",
                "FIRST"
            ]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec![
                "FOLSET",
                "risk-fol-str",
                "last",
                "30",
                "60000",
                "FIRST"
            ]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["FOLQUERY", "risk-fol-str"]),
            RespValue::Bulk(Some(b"first".to_vec()))
        );
        assert_eq!(
            run(vec![
                "FOLSET",
                "risk-fol-last",
                "middle",
                "20",
                "60000",
                "LAST"
            ]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec![
                "FOLSET",
                "risk-fol-last",
                "first",
                "10",
                "60000",
                "LAST"
            ]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec![
                "FOLSET",
                "risk-fol-last",
                "last",
                "30",
                "60000",
                "LAST"
            ]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["FOLQUERY", "risk-fol-last"]),
            RespValue::Bulk(Some(b"last".to_vec()))
        );
    }

    #[test]
    fn redis_feature_query_filterstr_uses_cpp_filter_syntax() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let matching = SequenceFeatureRow {
            timestamp_ms: 10,
            gid: 7,
            action_type: 1,
            duration: 5,
            author_id: 99,
        };
        let other = SequenceFeatureRow {
            timestamp_ms: 20,
            gid: 8,
            action_type: 1,
            duration: 9,
            author_id: 99,
        };
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "feature-pb".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: matching.timestamp_ms,
                        value: matching.encode_cpp_feature_value(),
                    },
                    FeaturePoint {
                        timestamp_ms: other.timestamp_ms,
                        value: other.encode_cpp_feature_value(),
                    },
                ],
            },
        });
        let run = |args: Vec<&str>| {
            execute_redis_command(
                args.into_iter()
                    .map(|arg| arg.as_bytes().to_vec())
                    .collect(),
                1,
                |command| {
                    let response = engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };

        assert_eq!(
            run(vec![
                "FQUERYFILTERSTR",
                "feature-pb",
                "0",
                "30",
                "10",
                "gid = 7",
                "duration < 6",
            ]),
            RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(10),
                RespValue::Bulk(Some(matching.encode_cpp_feature_value())),
            ])])
        );
        assert_eq!(
            run(vec![
                "FQUERYFILTERSTR",
                "feature-pb",
                "0",
                "30",
                "10",
                "gid >= 7",
            ]),
            RespValue::Array(vec![
                RespValue::Array(vec![
                    RespValue::Integer(10),
                    RespValue::Bulk(Some(matching.encode_cpp_feature_value())),
                ]),
                RespValue::Array(vec![
                    RespValue::Integer(20),
                    RespValue::Bulk(Some(other.encode_cpp_feature_value())),
                ]),
            ])
        );
        assert_eq!(
            run(vec![
                "FQUERYFILTERSTR",
                "feature-pb",
                "0",
                "30",
                "10",
                "duration <= 5",
            ]),
            RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Integer(10),
                RespValue::Bulk(Some(matching.encode_cpp_feature_value())),
            ])])
        );
    }

    #[test]
    fn redis_operational_commands_match_cpp_server_shape() {
        let mut state = RedisCommandState::default();
        let run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter()
                    .map(|arg| arg.as_bytes().to_vec())
                    .collect(),
                7,
                state,
                |_| Err("unexpected data command".to_string()),
            )
        };

        assert_eq!(
            run(&mut state, vec!["CONFIG", "GET", "requirepass"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"requirepass".to_vec())),
                RespValue::Bulk(Some(Vec::new())),
            ])
        );
        assert_eq!(
            run(&mut state, vec!["CONFIG", "SET", "requirepass", "secret"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["AUTH", "bad"]),
            RespValue::Error("ERR invalid password".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["AUTH", "secret"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert!(state.authenticated);
        assert_eq!(
            run(&mut state, vec!["ECHO", "hello"]),
            RespValue::Bulk(Some(b"hello".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["SELECT", "0"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["SELECT", "1"]),
            RespValue::Error("ERR DB index is out of range".to_string())
        );

        assert_eq!(
            run(&mut state, vec!["SLAVEOF", "127.0.0.1", "18001"]),
            RespValue::SimpleString("OK".to_string())
        );
        let info = run(&mut state, vec!["INFO", "replication"]);
        match info {
            RespValue::Bulk(Some(bytes)) => {
                let text = String::from_utf8(bytes).unwrap();
                assert!(text.contains("role:slave"));
                assert!(text.contains("master_host:127.0.0.1"));
                assert!(text.contains("master_port:18001"));
            }
            other => panic!("unexpected info response: {other:?}"),
        }
        assert_eq!(
            run(&mut state, vec!["SLAVEOF", "NO", "ONE"]),
            RespValue::SimpleString("OK".to_string())
        );
        let info = run(&mut state, vec!["INFO", "replication"]);
        match info {
            RespValue::Bulk(Some(bytes)) => {
                assert!(String::from_utf8(bytes).unwrap().contains("role:master"));
            }
            other => panic!("unexpected info response: {other:?}"),
        }

        assert_eq!(
            run(
                &mut state,
                vec!["PARTITION", "LOAD", "7", "1", "file:///tmp/partition"]
            ),
            RespValue::SimpleString("OK".to_string())
        );
        let partition = run(&mut state, vec!["PARTITION", "INFO"]);
        match partition {
            RespValue::Bulk(Some(bytes)) => {
                let text = String::from_utf8(bytes).unwrap();
                assert!(text.contains("partition_id:7"));
                assert!(text.contains("partition_loading_stats:loaded"));
            }
            other => panic!("unexpected partition info response: {other:?}"),
        }
        assert_eq!(
            run(&mut state, vec!["BGSAVE"]),
            RespValue::SimpleString("Background saving started".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["CONFIG", "REWRITE"]),
            RespValue::SimpleString("OK".to_string())
        );
    }

    #[test]
    fn redis_slot_hash_commands_use_cpp_crc64_formula() {
        let mut state = RedisCommandState::default();
        let mut run = |args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter()
                    .map(|arg| arg.as_bytes().to_vec())
                    .collect(),
                1,
                &mut state,
                |_| Err("unexpected data command".to_string()),
            )
        };

        assert_eq!(
            run(vec!["PSLOTHASHKEY", "123456789"]),
            RespValue::Integer(0x3a71_b645)
        );
        assert_eq!(
            run(vec!["PCLUSTERKEYSLOT", "123456789"]),
            RespValue::Integer(0x3a71_b645)
        );
        assert_eq!(
            run(vec!["PCLUSTERHASH", "123456789"]),
            RespValue::Integer(0xe9c6_d914_c4b8_d9cau64 as i64)
        );
    }
}
