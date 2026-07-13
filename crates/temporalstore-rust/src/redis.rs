use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

mod encoding;
mod protocol;
mod server;
mod state;

pub use protocol::{read_command, RespValue};
pub use server::serve_redis_proxy;
pub use state::RedisCommandState;

use crate::client::{slot_id_for_key, stable_key_hash};
use crate::types::{
    parse_cpp_feature_filters, Command, CommandResponse, FeatureFilter, FeatureFilterOp,
    FeaturePoint, FeatureWritePolicy, RiskFamily, RiskFolType, ShardId, StringSetCondition,
};

use encoding::{REDIS_LIST_ENCODING_PREFIX, REDIS_ZSET_ENCODING_PREFIX};

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
    state.total_commands_processed = state.total_commands_processed.saturating_add(1);
    if open_source_redis_surface_enabled() && !open_source_redis_command_allowed(&command) {
        state.rejected_commands = state.rejected_commands.saturating_add(1);
        state.open_source_rejected_commands = state.open_source_rejected_commands.saturating_add(1);
        return RespValue::Error(format!(
            "ERR command {command} is not part of the open-source Redis surface"
        ));
    }
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
        "QUIT" if args.len() == 1 => RespValue::SimpleString("OK".to_string()),
        "CLIENT" if args.len() >= 2 => redis_client_response(&args),
        "SELECT" if args.len() == 2 => match parse_u64(&args[1], "db") {
            Ok(0) => RespValue::SimpleString("OK".to_string()),
            Ok(_) => RespValue::Error("ERR DB index is out of range".to_string()),
            Err(err) => RespValue::Error(err),
        },
        "BGSAVE" if args.len() == 1 || args.len() == 2 => {
            RespValue::SimpleString("Background saving started".to_string())
        }
        "COMMAND" => redis_command_response(&args),
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
        "DBSIZE" if args.len() == 1 => RespValue::Integer(state.keyspace.len() as i64),
        "TYPE" if args.len() == 2 => redis_type_response(&args[1], &mut execute),
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
                        state.keyspace.remove(&string_arg(&args[1]));
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
                        key: key.clone(),
                        value: args[2].clone(),
                    }) {
                        return RespValue::Error(format!("ERR {err}"));
                    }
                    state.keyspace.insert(key);
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
                key: key.clone(),
                value,
                ttl_ms: options.ttl_ms,
                condition: options.condition,
                return_old: options.return_old,
            }) {
                Ok(CommandResponse::Bytes { value }) => {
                    if options.condition == StringSetCondition::Always || value.is_some() {
                        state.keyspace.insert(key);
                    }
                    RespValue::Bulk(value)
                }
                Ok(CommandResponse::Integer { value: 1 }) => {
                    state.keyspace.insert(key);
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
            Ok(CommandResponse::Integer { value }) => {
                if value > 0 {
                    state.keyspace.insert(string_arg(&args[1]));
                }
                RespValue::Integer(value)
            }
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
                state.keyspace.insert(string_arg(&pair[0]));
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
                state.keyspace.insert(string_arg(&pair[0]));
            }
            RespValue::Integer(1)
        }
        "GETEX" if args.len() >= 2 => {
            let key = string_arg(&args[1]);
            match execute(Command::StringGet { key: key.clone() }) {
                Ok(CommandResponse::Bytes { value }) => {
                    if value.is_some() {
                        let ttl_ms = match parse_getex_ttl_ms(&args[2..]) {
                            Ok(value) => value,
                            Err(err) => return RespValue::Error(err),
                        };
                        if let Some(ttl_ms) = ttl_ms {
                            if let Err(err) = execute(Command::CommonExpire { key, ttl_ms }) {
                                return RespValue::Error(format!("ERR {err}"));
                            }
                        }
                    }
                    RespValue::Bulk(value)
                }
                Ok(_) => RespValue::Error("ERR invalid getex response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "SETEX" if args.len() == 4 => match parse_u64(&args[2], "seconds") {
            Ok(seconds) => match execute(Command::StringSetEx {
                key: string_arg(&args[1]),
                value: args[3].clone(),
                ttl_ms: seconds.saturating_mul(1000),
            }) {
                Ok(_) => {
                    state.keyspace.insert(string_arg(&args[1]));
                    RespValue::SimpleString("OK".to_string())
                }
                Err(err) => RespValue::Error(format!("ERR {err}")),
            },
            Err(err) => RespValue::Error(err),
        },
        "PSETEX" if args.len() == 4 => match parse_u64(&args[2], "milliseconds") {
            Ok(ttl_ms) => match execute(Command::StringSetEx {
                key: string_arg(&args[1]),
                value: args[3].clone(),
                ttl_ms,
            }) {
                Ok(_) => {
                    state.keyspace.insert(string_arg(&args[1]));
                    RespValue::SimpleString("OK".to_string())
                }
                Err(err) => RespValue::Error(format!("ERR {err}")),
            },
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
            for key_arg in args.iter().skip(1) {
                let key = string_arg(key_arg);
                match execute(Command::CommonExists { key: key.clone() }) {
                    Ok(CommandResponse::Integer { value }) => {
                        if value > 0 {
                            if let Err(err) = execute(Command::CommonDelete { key: key.clone() }) {
                                return RespValue::Error(format!("ERR {err}"));
                            }
                            state.keyspace.remove(&key);
                            removed += 1;
                        }
                    }
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Integer(removed)
        }
        "UNLINK" if args.len() >= 2 => {
            let mut removed = 0;
            for key_arg in args.iter().skip(1) {
                let key = string_arg(key_arg);
                match execute(Command::CommonExists { key: key.clone() }) {
                    Ok(CommandResponse::Integer { value }) if value > 0 => {
                        if let Err(err) = execute(Command::CommonDelete { key: key.clone() }) {
                            return RespValue::Error(format!("ERR {err}"));
                        }
                        state.keyspace.remove(&key);
                        removed += 1;
                    }
                    Ok(CommandResponse::Integer { .. }) => {}
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Integer(removed)
        }
        "TOUCH" if args.len() >= 2 => {
            let mut touched = 0;
            for key in args.iter().skip(1) {
                match execute(Command::CommonExists {
                    key: string_arg(key),
                }) {
                    Ok(CommandResponse::Integer { value }) => touched += value,
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Integer(touched)
        }
        "RANDOMKEY" if args.len() == 1 => sorted_matching_keys("*", state)
            .into_iter()
            .next()
            .map(|key| RespValue::Bulk(Some(key.into_bytes())))
            .unwrap_or(RespValue::Bulk(None)),
        "FLUSHDB" | "FLUSHALL" if args.len() == 1 || args.len() == 2 => {
            for key in state.keyspace.iter().cloned().collect::<Vec<_>>() {
                if let Err(err) = execute(Command::CommonDelete { key }) {
                    return RespValue::Error(format!("ERR {err}"));
                }
            }
            state.keyspace.clear();
            RespValue::SimpleString("OK".to_string())
        }
        "COPY" if args.len() == 3 || args.len() == 4 => {
            let replace = args
                .get(3)
                .is_some_and(|option| upper(option).as_str() == "REPLACE");
            copy_or_rename_key_response(&args[1], &args[2], false, replace, state, &mut execute)
        }
        "RENAME" if args.len() == 3 => {
            copy_or_rename_key_response(&args[1], &args[2], true, true, state, &mut execute)
        }
        "RENAMENX" if args.len() == 3 => {
            copy_or_rename_key_response(&args[1], &args[2], true, false, state, &mut execute)
        }
        "EXPIRE" if args.len() == 3 => expire_response(&args, 1000, execute),
        "PEXPIRE" if args.len() == 3 => expire_response(&args, 1, execute),
        "EXPIREAT" if args.len() == 3 => expire_at_response(&args, 1000, execute),
        "PEXPIREAT" if args.len() == 3 => expire_at_response(&args, 1, execute),
        "EXPIRETIME" if args.len() == 2 => expire_time_response(&args[1], 1000, &mut execute),
        "PEXPIRETIME" if args.len() == 2 => expire_time_response(&args[1], 1, &mut execute),
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
        "GETRANGE" if args.len() == 4 => string_getrange_response(&args, &mut execute),
        "SETRANGE" if args.len() == 4 => {
            let response = string_setrange_response(&args, &mut execute);
            if matches!(response, RespValue::Integer(_)) {
                state.keyspace.insert(string_arg(&args[1]));
            }
            response
        }
        "APPEND" if args.len() == 3 => {
            let key = string_arg(&args[1]);
            match execute(Command::StringGet { key: key.clone() }) {
                Ok(CommandResponse::Bytes { value }) => {
                    let mut new_value = value.unwrap_or_default();
                    new_value.extend_from_slice(&args[2]);
                    let new_len = new_value.len() as i64;
                    if let Err(err) = execute(Command::StringSet {
                        key: key.clone(),
                        value: new_value,
                    }) {
                        return RespValue::Error(format!("ERR {err}"));
                    }
                    state.keyspace.insert(key);
                    RespValue::Integer(new_len)
                }
                Ok(_) => RespValue::Error("ERR invalid append response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "INCR" if args.len() == 2 => {
            let response = string_increment_response(&args[1], 1, &mut execute);
            if matches!(response, RespValue::Integer(_)) {
                state.keyspace.insert(string_arg(&args[1]));
            }
            response
        }
        "DECR" if args.len() == 2 => {
            let response = string_increment_response(&args[1], -1, &mut execute);
            if matches!(response, RespValue::Integer(_)) {
                state.keyspace.insert(string_arg(&args[1]));
            }
            response
        }
        "INCRBY" if args.len() == 3 => match parse_i64_arg(&args[2], "increment") {
            Ok(increment) => {
                let response = string_increment_response(&args[1], increment, &mut execute);
                if matches!(response, RespValue::Integer(_)) {
                    state.keyspace.insert(string_arg(&args[1]));
                }
                response
            }
            Err(err) => RespValue::Error(err),
        },
        "INCRBYFLOAT" if args.len() == 3 => {
            let response = string_increment_float_response(&args[1], &args[2], &mut execute);
            if matches!(response, RespValue::Bulk(Some(_))) {
                state.keyspace.insert(string_arg(&args[1]));
            }
            response
        }
        "DECRBY" if args.len() == 3 => match parse_i64_arg(&args[2], "decrement") {
            Ok(decrement) => {
                let response = string_increment_response(&args[1], -decrement, &mut execute);
                if matches!(response, RespValue::Integer(_)) {
                    state.keyspace.insert(string_arg(&args[1]));
                }
                response
            }
            Err(err) => RespValue::Error(err),
        },
        "HINCRBYFLOAT" if args.len() == 4 => {
            let response =
                hash_increment_float_response(&args[1], &args[2], &args[3], &mut execute);
            if matches!(response, RespValue::Bulk(Some(_))) {
                state.keyspace.insert(string_arg(&args[1]));
            }
            response
        }
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
            match execute(Command::HashMultiSet {
                key: key.clone(),
                entries,
            }) {
                Ok(_) => {
                    state.keyspace.insert(key);
                    RespValue::Integer(added)
                }
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "HSETNX" if args.len() == 4 => match execute(Command::HashGet {
            key: string_arg(&args[1]),
            field: string_arg(&args[2]),
        }) {
            Ok(CommandResponse::Bytes { value: Some(_) }) => RespValue::Integer(0),
            Ok(CommandResponse::Bytes { value: None }) => match execute(Command::HashSet {
                key: string_arg(&args[1]),
                field: string_arg(&args[2]),
                value: args[3].clone(),
            }) {
                Ok(_) => {
                    state.keyspace.insert(string_arg(&args[1]));
                    RespValue::Integer(1)
                }
                Err(err) => RespValue::Error(format!("ERR {err}")),
            },
            Ok(_) => RespValue::Error("ERR invalid hget response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "HMSET" if args.len() >= 4 && args.len() % 2 == 0 => {
            let entries = args[2..]
                .chunks(2)
                .map(|pair| (string_arg(&pair[0]), pair[1].clone()))
                .collect();
            match execute(Command::HashMultiSet {
                key: string_arg(&args[1]),
                entries,
            }) {
                Ok(_) => {
                    state.keyspace.insert(string_arg(&args[1]));
                    RespValue::SimpleString("OK".to_string())
                }
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "HGET" if args.len() == 3 => bytes_response(execute(Command::HashGet {
            key: string_arg(&args[1]),
            field: string_arg(&args[2]),
        })),
        "HSTRLEN" if args.len() == 3 => match execute(Command::HashGet {
            key: string_arg(&args[1]),
            field: string_arg(&args[2]),
        }) {
            Ok(CommandResponse::Bytes { value }) => {
                RespValue::Integer(value.map(|value| value.len() as i64).unwrap_or_default())
            }
            Ok(_) => RespValue::Error("ERR invalid hstrlen response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "HEXISTS" if args.len() == 3 => match execute(Command::HashGet {
            key: string_arg(&args[1]),
            field: string_arg(&args[2]),
        }) {
            Ok(CommandResponse::Bytes { value }) => RespValue::Integer(i64::from(value.is_some())),
            Ok(_) => RespValue::Error("ERR invalid hget response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
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
        "HKEYS" if args.len() == 2 => match execute(Command::HashGetAll {
            key: string_arg(&args[1]),
        }) {
            Ok(CommandResponse::HashEntries { entries }) => RespValue::Array(
                entries
                    .into_iter()
                    .map(|(field, _)| RespValue::Bulk(Some(field.into_bytes())))
                    .collect(),
            ),
            Ok(_) => RespValue::Error("ERR invalid hkeys response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "HVALS" if args.len() == 2 => match execute(Command::HashGetAll {
            key: string_arg(&args[1]),
        }) {
            Ok(CommandResponse::HashEntries { entries }) => RespValue::Array(
                entries
                    .into_iter()
                    .map(|(_, value)| RespValue::Bulk(Some(value)))
                    .collect(),
            ),
            Ok(_) => RespValue::Error("ERR invalid hvals response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        "HSCAN" if args.len() >= 3 => {
            let cursor = match parse_usize(&args[2], "cursor") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let (pattern, count) = match parse_scan_tail_options(&args[3..]) {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            match execute(Command::HashGetAll {
                key: string_arg(&args[1]),
            }) {
                Ok(CommandResponse::HashEntries { mut entries }) => {
                    entries.sort_by(|left, right| left.0.cmp(&right.0));
                    let values = entries
                        .into_iter()
                        .filter(|(field, _)| redis_pattern_matches(&pattern, field))
                        .flat_map(|(field, value)| {
                            [
                                RespValue::Bulk(Some(field.into_bytes())),
                                RespValue::Bulk(Some(value)),
                            ]
                        })
                        .collect();
                    redis_cursor_page_response(cursor, count, values)
                }
                Ok(_) => RespValue::Error("ERR invalid hscan response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
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
                    state.keyspace.insert(key.clone());
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
        "SPOP" if args.len() == 2 || args.len() == 3 => {
            let key = string_arg(&args[1]);
            let count = match args.get(2) {
                Some(value) => match parse_usize(value, "count") {
                    Ok(value) => Some(value),
                    Err(err) => return RespValue::Error(err),
                },
                None => None,
            };
            let members = match sorted_set_members(&key, &mut execute) {
                Ok(members) => members,
                Err(err) => return RespValue::Error(err),
            };
            let selected = members
                .into_iter()
                .take(count.unwrap_or(1))
                .collect::<Vec<_>>();
            for member in &selected {
                if let Err(err) = execute(Command::SetRemove {
                    key: key.clone(),
                    member: member.clone(),
                }) {
                    return RespValue::Error(format!("ERR {err}"));
                }
            }
            match count {
                Some(_) => RespValue::Array(
                    selected
                        .into_iter()
                        .map(|member| RespValue::Bulk(Some(member)))
                        .collect(),
                ),
                None => RespValue::Bulk(selected.into_iter().next()),
            }
        }
        "SRANDMEMBER" if args.len() == 2 || args.len() == 3 => {
            let count = match args.get(2) {
                Some(value) => match parse_i64_arg(value, "count") {
                    Ok(value) => Some(value),
                    Err(err) => return RespValue::Error(err),
                },
                None => None,
            };
            match sorted_set_members(&string_arg(&args[1]), &mut execute) {
                Ok(members) => match count {
                    Some(count) if count >= 0 => RespValue::Array(
                        members
                            .into_iter()
                            .take(count as usize)
                            .map(|member| RespValue::Bulk(Some(member)))
                            .collect(),
                    ),
                    Some(count) => {
                        let take = count.unsigned_abs() as usize;
                        let mut out = Vec::new();
                        for index in 0..take {
                            if members.is_empty() {
                                break;
                            }
                            out.push(RespValue::Bulk(Some(
                                members[index % members.len()].clone(),
                            )));
                        }
                        RespValue::Array(out)
                    }
                    None => RespValue::Bulk(members.into_iter().next()),
                },
                Err(err) => RespValue::Error(err),
            }
        }
        "SSCAN" if args.len() >= 3 => {
            let cursor = match parse_usize(&args[2], "cursor") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let (pattern, count) = match parse_scan_tail_options(&args[3..]) {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            match sorted_set_members(&string_arg(&args[1]), &mut execute) {
                Ok(members) => redis_cursor_page_response(
                    cursor,
                    count,
                    members
                        .into_iter()
                        .filter(|member| redis_pattern_matches(&pattern, &string_arg(member)))
                        .map(|member| RespValue::Bulk(Some(member)))
                        .collect(),
                ),
                Err(err) => RespValue::Error(err),
            }
        }
        "SMOVE" if args.len() == 4 => {
            let source = string_arg(&args[1]);
            let destination = string_arg(&args[2]);
            let member = args[3].clone();
            match execute(Command::SetMembers {
                key: source.clone(),
            }) {
                Ok(CommandResponse::Members { members }) if members.contains(&member) => {
                    if let Err(err) = execute(Command::SetRemove {
                        key: source,
                        member: member.clone(),
                    }) {
                        return RespValue::Error(format!("ERR {err}"));
                    }
                    if let Err(err) = execute(Command::SetAdd {
                        key: destination.clone(),
                        member,
                    }) {
                        return RespValue::Error(format!("ERR {err}"));
                    }
                    state.keyspace.insert(destination);
                    RespValue::Integer(1)
                }
                Ok(CommandResponse::Members { .. }) => RespValue::Integer(0),
                Ok(_) => RespValue::Error("ERR invalid smove response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "SDIFF" if args.len() >= 2 => {
            set_algebra_response(&args[1..], SetAlgebraOp::Diff, &mut execute)
        }
        "SINTER" if args.len() >= 2 => {
            set_algebra_response(&args[1..], SetAlgebraOp::Inter, &mut execute)
        }
        "SUNION" if args.len() >= 2 => {
            set_algebra_response(&args[1..], SetAlgebraOp::Union, &mut execute)
        }
        "LPUSH" if args.len() >= 3 => {
            let response = list_push_response(&args, true, &mut execute);
            if matches!(response, RespValue::Integer(_)) {
                state.keyspace.insert(string_arg(&args[1]));
            }
            response
        }
        "RPUSH" if args.len() >= 3 => {
            let response = list_push_response(&args, false, &mut execute);
            if matches!(response, RespValue::Integer(_)) {
                state.keyspace.insert(string_arg(&args[1]));
            }
            response
        }
        "LPOP" if args.len() == 2 || args.len() == 3 => {
            list_pop_response(&args, true, &mut execute)
        }
        "RPOP" if args.len() == 2 || args.len() == 3 => {
            list_pop_response(&args, false, &mut execute)
        }
        "LREM" if args.len() == 4 => {
            let response = list_rem_response(&args, &mut execute);
            if matches!(response, RespValue::Integer(_)) {
                state.keyspace.insert(string_arg(&args[1]));
            }
            response
        }
        "RPOPLPUSH" if args.len() == 3 => {
            let response = list_move_response(&args[1], &args[2], false, true, &mut execute);
            if matches!(response, RespValue::Bulk(Some(_))) {
                state.keyspace.insert(string_arg(&args[1]));
                state.keyspace.insert(string_arg(&args[2]));
            }
            response
        }
        "LMOVE" if args.len() == 5 => {
            let from_left = match parse_list_side(&args[3]) {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let to_left = match parse_list_side(&args[4]) {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let response = list_move_response(&args[1], &args[2], from_left, to_left, &mut execute);
            if matches!(response, RespValue::Bulk(Some(_))) {
                state.keyspace.insert(string_arg(&args[1]));
                state.keyspace.insert(string_arg(&args[2]));
            }
            response
        }
        "LLEN" if args.len() == 2 => match load_redis_list(&string_arg(&args[1]), &mut execute) {
            Ok(values) => RespValue::Integer(values.len() as i64),
            Err(err) => RespValue::Error(err),
        },
        "LINDEX" if args.len() == 3 => match parse_i64_arg(&args[2], "index") {
            Ok(index) => match load_redis_list(&string_arg(&args[1]), &mut execute) {
                Ok(values) => {
                    let index = normalize_index(index, values.len());
                    RespValue::Bulk(index.and_then(|index| values.get(index).cloned()))
                }
                Err(err) => RespValue::Error(err),
            },
            Err(err) => RespValue::Error(err),
        },
        "LPOS" if args.len() == 3 => match load_redis_list(&string_arg(&args[1]), &mut execute) {
            Ok(values) => values
                .iter()
                .position(|value| value == &args[2])
                .map(|index| RespValue::Integer(index as i64))
                .unwrap_or(RespValue::Bulk(None)),
            Err(err) => RespValue::Error(err),
        },
        "LINSERT" if args.len() == 5 => {
            list_insert_response(&args[1], &args[2], &args[3], &args[4], &mut execute)
        }
        "LRANGE" if args.len() == 4 => {
            let start = match parse_i64_arg(&args[2], "start") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let stop = match parse_i64_arg(&args[3], "stop") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            match load_redis_list(&string_arg(&args[1]), &mut execute) {
                Ok(values) => {
                    let (start, stop) = normalize_range(start, stop, values.len());
                    RespValue::Array(
                        values[start..stop]
                            .iter()
                            .cloned()
                            .map(|value| RespValue::Bulk(Some(value)))
                            .collect(),
                    )
                }
                Err(err) => RespValue::Error(err),
            }
        }
        "LSET" if args.len() == 4 => {
            let key = string_arg(&args[1]);
            let index = match parse_i64_arg(&args[2], "index") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            match load_redis_list(&key, &mut execute) {
                Ok(mut values) => match normalize_index(index, values.len()) {
                    Some(index) => {
                        values[index] = args[3].clone();
                        match store_redis_list(&key, &values, &mut execute) {
                            Ok(()) => {
                                state.keyspace.insert(key);
                                RespValue::SimpleString("OK".to_string())
                            }
                            Err(err) => RespValue::Error(err),
                        }
                    }
                    None => RespValue::Error("ERR index out of range".to_string()),
                },
                Err(err) => RespValue::Error(err),
            }
        }
        "LTRIM" if args.len() == 4 => {
            let key = string_arg(&args[1]);
            let start = match parse_i64_arg(&args[2], "start") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let stop = match parse_i64_arg(&args[3], "stop") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            match load_redis_list(&key, &mut execute) {
                Ok(values) => {
                    let (start, stop) = normalize_range(start, stop, values.len());
                    let trimmed = values[start..stop].to_vec();
                    match store_redis_list(&key, &trimmed, &mut execute) {
                        Ok(()) => {
                            state.keyspace.insert(key);
                            RespValue::SimpleString("OK".to_string())
                        }
                        Err(err) => RespValue::Error(err),
                    }
                }
                Err(err) => RespValue::Error(err),
            }
        }
        "ZADD" if args.len() >= 4 && args.len() % 2 == 0 => {
            let key = string_arg(&args[1]);
            let mut values = match load_redis_zset(&key, &mut execute) {
                Ok(values) => values,
                Err(err) => return RespValue::Error(err),
            };
            let mut added = 0;
            for pair in args[2..].chunks(2) {
                let score = match parse_f64_arg(&pair[0], "score") {
                    Ok(value) => value,
                    Err(err) => return RespValue::Error(err),
                };
                if upsert_zset_member(&mut values, pair[1].clone(), score) {
                    added += 1;
                }
            }
            match store_redis_zset(&key, &values, &mut execute) {
                Ok(()) => {
                    state.keyspace.insert(key);
                    RespValue::Integer(added)
                }
                Err(err) => RespValue::Error(err),
            }
        }
        "ZRANGE" if args.len() == 4 || args.len() == 5 => {
            zrange_response(&args, false, &mut execute)
        }
        "ZREVRANGE" if args.len() == 4 || args.len() == 5 => {
            zrange_response(&args, true, &mut execute)
        }
        "ZCARD" if args.len() == 2 => match load_redis_zset(&string_arg(&args[1]), &mut execute) {
            Ok(values) => RespValue::Integer(values.len() as i64),
            Err(err) => RespValue::Error(err),
        },
        "ZSCORE" if args.len() == 3 => match load_redis_zset(&string_arg(&args[1]), &mut execute) {
            Ok(values) => RespValue::Bulk(
                values
                    .iter()
                    .find(|(member, _)| member == &args[2])
                    .map(|(_, score)| format_redis_score(*score).into_bytes()),
            ),
            Err(err) => RespValue::Error(err),
        },
        "ZMSCORE" if args.len() >= 3 => {
            match load_redis_zset(&string_arg(&args[1]), &mut execute) {
                Ok(values) => RespValue::Array(
                    args.iter()
                        .skip(2)
                        .map(|member| {
                            RespValue::Bulk(
                                values
                                    .iter()
                                    .find(|(existing, _)| existing == member)
                                    .map(|(_, score)| format_redis_score(*score).into_bytes()),
                            )
                        })
                        .collect(),
                ),
                Err(err) => RespValue::Error(err),
            }
        }
        "ZRANDMEMBER" if args.len() == 2 || args.len() == 3 || args.len() == 4 => {
            zrandmember_response(&args, &mut execute)
        }
        "ZRANK" if args.len() == 3 => zrank_response(&args, false, &mut execute),
        "ZREVRANK" if args.len() == 3 => zrank_response(&args, true, &mut execute),
        "ZINCRBY" if args.len() == 4 => {
            let response = zincrby_response(&args, &mut execute);
            if matches!(response, RespValue::Bulk(Some(_))) {
                state.keyspace.insert(string_arg(&args[1]));
            }
            response
        }
        "ZREM" if args.len() >= 3 => {
            let key = string_arg(&args[1]);
            let mut values = match load_redis_zset(&key, &mut execute) {
                Ok(values) => values,
                Err(err) => return RespValue::Error(err),
            };
            let before = values.len();
            values.retain(|(member, _)| !args[2..].iter().any(|arg| arg == member));
            let removed = before.saturating_sub(values.len()) as i64;
            match store_redis_zset(&key, &values, &mut execute) {
                Ok(()) => RespValue::Integer(removed),
                Err(err) => RespValue::Error(err),
            }
        }
        "ZPOPMIN" if args.len() == 2 || args.len() == 3 => {
            zpop_response(&args, false, &mut execute)
        }
        "ZPOPMAX" if args.len() == 2 || args.len() == 3 => zpop_response(&args, true, &mut execute),
        "ZCOUNT" if args.len() == 4 => {
            let min = match parse_f64_arg(&args[2], "min") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let max = match parse_f64_arg(&args[3], "max") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            match load_redis_zset(&string_arg(&args[1]), &mut execute) {
                Ok(values) => RespValue::Integer(
                    values
                        .iter()
                        .filter(|(_, score)| *score >= min && *score <= max)
                        .count() as i64,
                ),
                Err(err) => RespValue::Error(err),
            }
        }
        "ZRANGEBYSCORE" if args.len() == 4 || args.len() == 5 => {
            zrange_by_score_response(&args, &mut execute)
        }
        "ZREMRANGEBYSCORE" if args.len() == 4 => zremrangebyscore_response(&args, &mut execute),
        "ZREMRANGEBYRANK" if args.len() == 4 => zremrangebyrank_response(&args, &mut execute),
        "ZSCAN" if args.len() >= 3 => {
            let cursor = match parse_usize(&args[2], "cursor") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let (pattern, count) = match parse_scan_tail_options(&args[3..]) {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            match load_redis_zset(&string_arg(&args[1]), &mut execute) {
                Ok(mut values) => {
                    sort_zset_values(&mut values);
                    let values = values
                        .into_iter()
                        .filter(|(member, _)| redis_pattern_matches(&pattern, &string_arg(member)))
                        .flat_map(|(member, score)| {
                            [
                                RespValue::Bulk(Some(member)),
                                RespValue::Bulk(Some(format_redis_score(score).into_bytes())),
                            ]
                        })
                        .collect();
                    redis_cursor_page_response(cursor, count, values)
                }
                Err(err) => RespValue::Error(err),
            }
        }
        "ZDIFF" if args.len() >= 3 => {
            zset_algebra_response(&args, ZSetAlgebraOp::Diff, &mut execute)
        }
        "ZINTER" if args.len() >= 3 => {
            zset_algebra_response(&args, ZSetAlgebraOp::Inter, &mut execute)
        }
        "ZUNION" if args.len() >= 3 => {
            zset_algebra_response(&args, ZSetAlgebraOp::Union, &mut execute)
        }
        "KEYS" if args.len() == 2 => redis_keys_response(&args[1], state),
        "SCAN" if args.len() >= 2 => redis_scan_response(&args, state),
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
                precision_ms: None,
                ttl_ms: None,
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
                precision_ms: None,
                ttl_ms: None,
            }))
        }
        "RISKMANAGER" if args.len() == 2 => hash_entries_response(execute(Command::RiskManager {
            key: string_arg(&args[1]),
            op_type: None,
            field_list: Vec::new(),
            start_offset: String::new(),
            end_offset: String::new(),
            is_cpc: None,
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
        _ => {
            state.rejected_commands = state.rejected_commands.saturating_add(1);
            state.unsupported_commands = state.unsupported_commands.saturating_add(1);
            RespValue::Error(format!("ERR unsupported command or arity: {command}"))
        }
    }
}

fn open_source_redis_surface_enabled() -> bool {
    std::env::var("TEMPORALSTORE_OPEN_SOURCE_SURFACE")
        .or_else(|_| std::env::var("TS_OPEN_SOURCE_SURFACE"))
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(false)
}

fn open_source_redis_command_allowed(command: &str) -> bool {
    matches!(
        command,
        "AUTH"
            | "PING"
            | "ECHO"
            | "QUIT"
            | "CLIENT"
            | "SELECT"
            | "COMMAND"
            | "CONFIG"
            | "INFO"
            | "DBSIZE"
            | "TYPE"
            | "GET"
            | "MGET"
            | "GETDEL"
            | "GETSET"
            | "GETEX"
            | "SET"
            | "SETNX"
            | "SETEX"
            | "PSETEX"
            | "MSET"
            | "MSETNX"
            | "EXISTS"
            | "DEL"
            | "UNLINK"
            | "TOUCH"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "EXPIRETIME"
            | "PEXPIRETIME"
            | "TTL"
            | "PTTL"
            | "PERSIST"
            | "STRLEN"
            | "GETRANGE"
            | "SETRANGE"
            | "APPEND"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "INCRBYFLOAT"
            | "HSET"
            | "HSETNX"
            | "HGET"
            | "HMGET"
            | "HMSET"
            | "HDEL"
            | "HEXISTS"
            | "HLEN"
            | "HGETALL"
            | "HKEYS"
            | "HVALS"
            | "HSTRLEN"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "HSCAN"
            | "FAPPEND"
            | "FAPPENDPOLICY"
            | "FQUERY"
            | "FQUERYFILTER"
            | "FQUERYFILTERSTR"
            | "FAGG"
            | "RISKINCR"
            | "RISKINCROPT"
            | "RISKCHANGE"
            | "RISKCOUNT"
            | "RISKQUERY"
            | "RISKDETAIL"
            | "RISKHSET"
            | "HCHANGE"
            | "HQUERY"
            | "HSETANDGET"
            | "CPCSET"
            | "CPCSETANDGET"
            | "FOLSET"
            | "FOLQUERY"
    )
}

fn redis_client_response(args: &[Vec<u8>]) -> RespValue {
    let subcommand = upper(&args[1]);
    match subcommand.as_str() {
        "SETNAME" if args.len() == 3 => RespValue::SimpleString("OK".to_string()),
        "GETNAME" if args.len() == 2 => RespValue::Bulk(None),
        "ID" if args.len() == 2 => RespValue::Integer(0),
        _ => RespValue::Error("ERR unsupported CLIENT subcommand".to_string()),
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

#[derive(Debug, Clone, PartialEq)]
enum RedisStoredValue {
    String(Vec<u8>),
    Hash(Vec<(String, Vec<u8>)>),
    Set(Vec<Vec<u8>>),
}

fn copy_or_rename_key_response(
    source: &[u8],
    destination: &[u8],
    rename: bool,
    replace: bool,
    state: &mut RedisCommandState,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let source_key = string_arg(source);
    let destination_key = string_arg(destination);
    let value = match load_stored_key_value(&source_key, execute) {
        Ok(Some(value)) => value,
        Ok(None) if rename => return RespValue::Error("ERR no such key".to_string()),
        Ok(None) => return RespValue::Integer(0),
        Err(err) => return RespValue::Error(err),
    };
    if !replace {
        match execute(Command::CommonExists {
            key: destination_key.clone(),
        }) {
            Ok(CommandResponse::Integer { value }) if value > 0 => {
                return if rename {
                    RespValue::Integer(0)
                } else {
                    RespValue::Integer(0)
                };
            }
            Ok(CommandResponse::Integer { .. }) => {}
            Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
            Err(err) => return RespValue::Error(format!("ERR {err}")),
        }
    }
    if let Err(err) = store_stored_key_value(&destination_key, value, execute) {
        return RespValue::Error(err);
    }
    state.keyspace.insert(destination_key);
    if rename {
        if let Err(err) = execute(Command::CommonDelete {
            key: source_key.clone(),
        }) {
            return RespValue::Error(format!("ERR {err}"));
        }
        state.keyspace.remove(&source_key);
        RespValue::SimpleString("OK".to_string())
    } else {
        RespValue::Integer(1)
    }
}

fn load_stored_key_value(
    key: &str,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Option<RedisStoredValue>, String> {
    match execute(Command::StringGet {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Bytes { value: Some(value) }) => {
            return Ok(Some(RedisStoredValue::String(value)));
        }
        Ok(CommandResponse::Bytes { value: None }) => {}
        Ok(_) => return Err("ERR invalid string response".to_string()),
        Err(err) => return Err(format!("ERR {err}")),
    }
    match execute(Command::HashGetAll {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::HashEntries { entries }) if !entries.is_empty() => {
            return Ok(Some(RedisStoredValue::Hash(entries)));
        }
        Ok(CommandResponse::HashEntries { .. }) => {}
        Ok(_) => return Err("ERR invalid hash response".to_string()),
        Err(err) => return Err(format!("ERR {err}")),
    }
    match execute(Command::SetMembers {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Members { members }) if !members.is_empty() => {
            return Ok(Some(RedisStoredValue::Set(members)));
        }
        Ok(CommandResponse::Members { .. }) => {}
        Ok(_) => return Err("ERR invalid set response".to_string()),
        Err(err) => return Err(format!("ERR {err}")),
    }
    Ok(None)
}

fn store_stored_key_value(
    key: &str,
    value: RedisStoredValue,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<(), String> {
    match value {
        RedisStoredValue::String(value) => execute(Command::StringSet {
            key: key.to_string(),
            value,
        })
        .map(|_| ())
        .map_err(|err| format!("ERR {err}")),
        RedisStoredValue::Hash(entries) => execute(Command::HashMultiSet {
            key: key.to_string(),
            entries,
        })
        .map(|_| ())
        .map_err(|err| format!("ERR {err}")),
        RedisStoredValue::Set(members) => {
            for member in members {
                execute(Command::SetAdd {
                    key: key.to_string(),
                    member,
                })
                .map_err(|err| format!("ERR {err}"))?;
            }
            Ok(())
        }
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

fn redis_command_response(args: &[Vec<u8>]) -> RespValue {
    match args.get(1).map(|value| upper(value)) {
        None => RespValue::Array(
            redis_public_commands()
                .iter()
                .map(|command| RespValue::Bulk(Some(command.name.as_bytes().to_vec())))
                .collect(),
        ),
        Some(subcommand) if subcommand == "COUNT" && args.len() == 2 => {
            RespValue::Integer(redis_public_commands().len() as i64)
        }
        Some(subcommand) if subcommand == "INFO" && args.len() >= 3 => {
            let commands = redis_public_commands();
            RespValue::Array(
                args.iter()
                    .skip(2)
                    .map(|name| {
                        let name = upper(name);
                        commands
                            .iter()
                            .find(|command| command.name == name)
                            .map(|command| {
                                RespValue::Array(vec![
                                    RespValue::Bulk(Some(command.name.as_bytes().to_vec())),
                                    RespValue::Integer(command.arity),
                                    RespValue::Array(
                                        command
                                            .flags
                                            .iter()
                                            .map(|flag| {
                                                RespValue::Bulk(Some(flag.as_bytes().to_vec()))
                                            })
                                            .collect(),
                                    ),
                                ])
                            })
                            .unwrap_or(RespValue::Bulk(None))
                    })
                    .collect(),
            )
        }
        _ => RespValue::Error("ERR syntax error".to_string()),
    }
}

#[derive(Debug, Clone, Copy)]
struct RedisCommandDescriptor {
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
}

fn redis_public_commands() -> Vec<&'static RedisCommandDescriptor> {
    redis_supported_commands()
        .iter()
        .filter(|command| {
            !open_source_redis_surface_enabled() || open_source_redis_command_allowed(command.name)
        })
        .collect()
}

fn redis_supported_commands() -> &'static [RedisCommandDescriptor] {
    const READ: &[&str] = &["readonly"];
    const WRITE: &[&str] = &["write"];
    const ADMIN: &[&str] = &["admin"];
    &[
        RedisCommandDescriptor {
            name: "APPEND",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "AUTH",
            arity: 2,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "BGSAVE",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "CLIENT",
            arity: -2,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "COMMAND",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "CONFIG",
            arity: -2,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "COPY",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "DBSIZE",
            arity: 1,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "DEL",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ECHO",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "EXISTS",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "EXPIRE",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "EXPIREAT",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "EXPIRETIME",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "FLUSHALL",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "FLUSHDB",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "GET",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "GETDEL",
            arity: 2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "GETEX",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "GETSET",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "GETRANGE",
            arity: 4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HDEL",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HGET",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HGETALL",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HEXISTS",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HINCRBY",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HINCRBYFLOAT",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HKEYS",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HLEN",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HMGET",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HMSET",
            arity: -4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HSET",
            arity: -4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HSCAN",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HSETNX",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HSTRLEN",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HVALS",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "INFO",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "FAPPEND",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "FAPPENDPOLICY",
            arity: 5,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "FQUERY",
            arity: -4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "FQUERYFILTER",
            arity: 8,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "FQUERYFILTERSTR",
            arity: -6,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "FAGG",
            arity: 5,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "RISKINCR",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "RISKINCROPT",
            arity: 6,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "RISKCHANGE",
            arity: -4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "RISKCOUNT",
            arity: 4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "RISKQUERY",
            arity: 5,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "RISKDETAIL",
            arity: -4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "RISKHSET",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HCHANGE",
            arity: -4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HQUERY",
            arity: 5,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HSETANDGET",
            arity: 7,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "CPCSET",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "CPCSETANDGET",
            arity: 7,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "FOLSET",
            arity: -4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "FOLQUERY",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "INCRBYFLOAT",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LINDEX",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "LLEN",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "LPOP",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LINSERT",
            arity: 5,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LMOVE",
            arity: 5,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LREM",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LPUSH",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LPOS",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "LRANGE",
            arity: 4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "LSET",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LTRIM",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "MGET",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "MSET",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "MSETNX",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "KEYS",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "PARTITION",
            arity: -2,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "PEXPIRE",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "PEXPIREAT",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "PEXPIRETIME",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "PING",
            arity: -1,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "PSETEX",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "PTTL",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "QUIT",
            arity: 1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "RANDOMKEY",
            arity: 1,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "RENAME",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "RENAMENX",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "RPOP",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "RPOPLPUSH",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "RPUSH",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SADD",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SCARD",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SDIFF",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SCAN",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SELECT",
            arity: 2,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "SET",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SETRANGE",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SETEX",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SETNX",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SISMEMBER",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SINTER",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SLAVEOF",
            arity: 3,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "SMEMBERS",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SMISMEMBER",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SMOVE",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SPOP",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SRANDMEMBER",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SREM",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SSCAN",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "STRLEN",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SUNION",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "TTL",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "TYPE",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "TOUCH",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "UNLINK",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ZADD",
            arity: -4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ZCARD",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZCOUNT",
            arity: 4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZDIFF",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZINCRBY",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ZINTER",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZMSCORE",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZPOPMAX",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ZPOPMIN",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ZRANGE",
            arity: -4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZRANGEBYSCORE",
            arity: -4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZREM",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ZREMRANGEBYRANK",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ZREMRANGEBYSCORE",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ZRANK",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZRANDMEMBER",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZREVRANGE",
            arity: -4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZREVRANK",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZSCAN",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZSCORE",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZUNION",
            arity: -3,
            flags: READ,
        },
    ]
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
            "# Stats\r\npartition_loading_stats:{loading}\r\ntotal_commands_processed:{}\r\nrejected_commands:{}\r\nopen_source_rejected_commands:{}\r\nunsupported_commands:{}\r\n",
            state.total_commands_processed,
            state.rejected_commands,
            state.open_source_rejected_commands,
            state.unsupported_commands
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

fn expire_at_response(
    args: &[Vec<u8>],
    factor: u64,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let deadline_ms = match parse_u64(&args[2], "timestamp") {
        Ok(value) => value.saturating_mul(factor),
        Err(err) => return RespValue::Error(err),
    };
    let ttl_ms = deadline_ms.saturating_sub(unix_time_ms()).max(1);
    match execute(Command::CommonExpire {
        key: string_arg(&args[1]),
        ttl_ms,
    }) {
        Ok(_) => RespValue::Integer(1),
        Err(err) if err.contains("not_found") || err.contains("key not found") => {
            RespValue::Integer(0)
        }
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn expire_time_response(
    key: &[u8],
    divisor_ms: u64,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    match execute(Command::CommonTtl {
        key: string_arg(key),
    }) {
        Ok(CommandResponse::Integer { value }) if value < 0 => RespValue::Integer(value),
        Ok(CommandResponse::Integer { value }) => {
            RespValue::Integer((unix_time_ms() as i64).saturating_add(value) / divisor_ms as i64)
        }
        Ok(_) => RespValue::Error("ERR invalid expiretime response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn parse_getex_ttl_ms(args: &[Vec<u8>]) -> Result<Option<u64>, String> {
    if args.is_empty() {
        return Ok(None);
    }
    if args.len() == 1 && upper(&args[0]) == "PERSIST" {
        return Ok(None);
    }
    if args.len() != 2 {
        return Err("ERR syntax error".to_string());
    }
    match upper(&args[0]).as_str() {
        "EX" => parse_u64(&args[1], "seconds").map(|value| Some(value.saturating_mul(1000))),
        "PX" => parse_u64(&args[1], "milliseconds").map(Some),
        "EXAT" => {
            let deadline = parse_u64(&args[1], "timestamp")?.saturating_mul(1000);
            Ok(Some(deadline.saturating_sub(unix_time_ms()).max(1)))
        }
        "PXAT" => {
            let deadline = parse_u64(&args[1], "timestamp")?;
            Ok(Some(deadline.saturating_sub(unix_time_ms()).max(1)))
        }
        _ => Err("ERR syntax error".to_string()),
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
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

fn string_increment_float_response(
    key: &[u8],
    increment: &[u8],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let increment = match parse_f64_arg(increment, "increment") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(key);
    let current = match execute(Command::StringGet { key: key.clone() }) {
        Ok(CommandResponse::Bytes { value: None }) => 0.0,
        Ok(CommandResponse::Bytes { value: Some(value) }) => match parse_f64_arg(&value, "value") {
            Ok(value) => value,
            Err(_) => return RespValue::Error("ERR value is not a valid float".to_string()),
        },
        Ok(_) => return RespValue::Error("ERR invalid incrbyfloat response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    };
    let value = format_redis_score(current + increment).into_bytes();
    match execute(Command::StringSet {
        key,
        value: value.clone(),
    }) {
        Ok(_) => RespValue::Bulk(Some(value)),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn hash_increment_float_response(
    key: &[u8],
    field: &[u8],
    increment: &[u8],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let increment = match parse_f64_arg(increment, "increment") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(key);
    let field = string_arg(field);
    let current = match execute(Command::HashGet {
        key: key.clone(),
        field: field.clone(),
    }) {
        Ok(CommandResponse::Bytes { value: None }) => 0.0,
        Ok(CommandResponse::Bytes { value: Some(value) }) => match parse_f64_arg(&value, "value") {
            Ok(value) => value,
            Err(_) => return RespValue::Error("ERR hash value is not a valid float".to_string()),
        },
        Ok(_) => return RespValue::Error("ERR invalid hincrbyfloat response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    };
    let value = format_redis_score(current + increment).into_bytes();
    match execute(Command::HashSet {
        key,
        field,
        value: value.clone(),
    }) {
        Ok(_) => RespValue::Bulk(Some(value)),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn list_push_response(
    args: &[Vec<u8>],
    left: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(&args[1]);
    let mut values = match load_redis_list(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    if left {
        for value in args.iter().skip(2) {
            values.insert(0, value.clone());
        }
    } else {
        values.extend(args.iter().skip(2).cloned());
    }
    let len = values.len() as i64;
    match store_redis_list(&key, &values, execute) {
        Ok(()) => RespValue::Integer(len),
        Err(err) => RespValue::Error(err),
    }
}

fn list_pop_response(
    args: &[Vec<u8>],
    left: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(&args[1]);
    let count = match args.get(2) {
        Some(value) => match parse_usize(value, "count") {
            Ok(value) => Some(value),
            Err(err) => return RespValue::Error(err),
        },
        None => None,
    };
    let mut values = match load_redis_list(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    if values.is_empty() {
        return if count.is_some() {
            RespValue::Array(Vec::new())
        } else {
            RespValue::Bulk(None)
        };
    }
    let pop_count = count.unwrap_or(1).min(values.len());
    let popped = if left {
        values.drain(..pop_count).collect::<Vec<_>>()
    } else {
        let split_at = values.len() - pop_count;
        values.split_off(split_at)
    };
    if let Err(err) = store_redis_list(&key, &values, execute) {
        return RespValue::Error(err);
    }
    match count {
        Some(_) => RespValue::Array(
            popped
                .into_iter()
                .map(|value| RespValue::Bulk(Some(value)))
                .collect(),
        ),
        None => RespValue::Bulk(popped.into_iter().next()),
    }
}

fn list_rem_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(&args[1]);
    let count = match parse_i64_arg(&args[2], "count") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let target = &args[3];
    let mut values = match load_redis_list(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    let before = values.len();
    if count == 0 {
        values.retain(|value| value != target);
    } else if count > 0 {
        let mut remaining = count as usize;
        values.retain(|value| {
            if remaining > 0 && value == target {
                remaining -= 1;
                false
            } else {
                true
            }
        });
    } else {
        let mut remaining = count.unsigned_abs() as usize;
        let mut reversed = values.into_iter().rev().collect::<Vec<_>>();
        reversed.retain(|value| {
            if remaining > 0 && value == target {
                remaining -= 1;
                false
            } else {
                true
            }
        });
        values = reversed.into_iter().rev().collect();
    }
    let removed = before.saturating_sub(values.len()) as i64;
    match store_redis_list(&key, &values, execute) {
        Ok(()) => RespValue::Integer(removed),
        Err(err) => RespValue::Error(err),
    }
}

fn parse_list_side(value: &[u8]) -> Result<bool, String> {
    match upper(value).as_str() {
        "LEFT" => Ok(true),
        "RIGHT" => Ok(false),
        _ => Err("ERR syntax error".to_string()),
    }
}

fn list_move_response(
    source: &[u8],
    destination: &[u8],
    from_left: bool,
    to_left: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let source_key = string_arg(source);
    let destination_key = string_arg(destination);
    let mut source_values = match load_redis_list(&source_key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    if source_values.is_empty() {
        return RespValue::Bulk(None);
    }
    let value = if from_left {
        source_values.remove(0)
    } else {
        source_values.pop().unwrap_or_default()
    };
    let mut destination_values = if source_key == destination_key {
        source_values.clone()
    } else {
        match load_redis_list(&destination_key, execute) {
            Ok(values) => values,
            Err(err) => return RespValue::Error(err),
        }
    };
    if to_left {
        destination_values.insert(0, value.clone());
    } else {
        destination_values.push(value.clone());
    }
    if source_key == destination_key {
        match store_redis_list(&source_key, &destination_values, execute) {
            Ok(()) => RespValue::Bulk(Some(value)),
            Err(err) => RespValue::Error(err),
        }
    } else if let Err(err) = store_redis_list(&source_key, &source_values, execute) {
        RespValue::Error(err)
    } else {
        match store_redis_list(&destination_key, &destination_values, execute) {
            Ok(()) => RespValue::Bulk(Some(value)),
            Err(err) => RespValue::Error(err),
        }
    }
}

fn list_insert_response(
    key: &[u8],
    placement: &[u8],
    pivot: &[u8],
    value: &[u8],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let before = match upper(placement).as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return RespValue::Error("ERR syntax error".to_string()),
    };
    let key = string_arg(key);
    let mut values = match load_redis_list(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    let Some(index) = values.iter().position(|existing| existing == pivot) else {
        return RespValue::Integer(-1);
    };
    let index = if before { index } else { index + 1 };
    values.insert(index, value.to_vec());
    let len = values.len() as i64;
    match store_redis_list(&key, &values, execute) {
        Ok(()) => RespValue::Integer(len),
        Err(err) => RespValue::Error(err),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetAlgebraOp {
    Diff,
    Inter,
    Union,
}

fn sorted_set_members(
    key: &str,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Vec<Vec<u8>>, String> {
    match execute(Command::SetMembers {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Members { mut members }) => {
            members.sort();
            Ok(members)
        }
        Ok(_) => Err("ERR invalid smembers response".to_string()),
        Err(err) => Err(format!("ERR {err}")),
    }
}

fn set_algebra_response(
    keys: &[Vec<u8>],
    op: SetAlgebraOp,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let mut sets = Vec::new();
    for key in keys {
        match sorted_set_members(&string_arg(key), execute) {
            Ok(members) => sets.push(members.into_iter().collect::<HashSet<_>>()),
            Err(err) => return RespValue::Error(err),
        }
    }
    let mut result = match op {
        SetAlgebraOp::Diff => sets.first().cloned().unwrap_or_default(),
        SetAlgebraOp::Inter => sets.first().cloned().unwrap_or_default(),
        SetAlgebraOp::Union => HashSet::new(),
    };
    match op {
        SetAlgebraOp::Diff => {
            for set in sets.iter().skip(1) {
                result.retain(|member| !set.contains(member));
            }
        }
        SetAlgebraOp::Inter => {
            for set in sets.iter().skip(1) {
                result.retain(|member| set.contains(member));
            }
        }
        SetAlgebraOp::Union => {
            for set in sets {
                result.extend(set);
            }
        }
    }
    let mut result = result.into_iter().collect::<Vec<_>>();
    result.sort();
    RespValue::Array(
        result
            .into_iter()
            .map(|member| RespValue::Bulk(Some(member)))
            .collect(),
    )
}

fn load_redis_list(
    key: &str,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Vec<Vec<u8>>, String> {
    match execute(Command::StringGet {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Bytes { value: None }) => Ok(Vec::new()),
        Ok(CommandResponse::Bytes { value: Some(value) }) => decode_redis_list(&value),
        Ok(_) => Err("ERR invalid list backing response".to_string()),
        Err(err) => Err(format!("ERR {err}")),
    }
}

fn store_redis_list(
    key: &str,
    values: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<(), String> {
    execute(Command::StringSet {
        key: key.to_string(),
        value: encode_redis_list(values),
    })
    .map(|_| ())
    .map_err(|err| format!("ERR {err}"))
}

fn encode_redis_list(values: &[Vec<u8>]) -> Vec<u8> {
    let mut out = REDIS_LIST_ENCODING_PREFIX.to_vec();
    out.extend_from_slice(&(values.len() as u64).to_be_bytes());
    for value in values {
        out.extend_from_slice(&(value.len() as u64).to_be_bytes());
        out.extend_from_slice(value);
    }
    out
}

fn decode_redis_list(value: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if !value.starts_with(REDIS_LIST_ENCODING_PREFIX) {
        return Err(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        );
    }
    let mut offset = REDIS_LIST_ENCODING_PREFIX.len();
    let count = read_u64_be(value, &mut offset)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u64_be(value, &mut offset)? as usize;
        let Some(end) = offset.checked_add(len) else {
            return Err("ERR corrupt list encoding".to_string());
        };
        if end > value.len() {
            return Err("ERR corrupt list encoding".to_string());
        }
        out.push(value[offset..end].to_vec());
        offset = end;
    }
    if offset != value.len() {
        return Err("ERR corrupt list encoding".to_string());
    }
    Ok(out)
}

fn string_getrange_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let start = match parse_i64_arg(&args[2], "start") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let stop = match parse_i64_arg(&args[3], "end") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    match execute(Command::StringGet {
        key: string_arg(&args[1]),
    }) {
        Ok(CommandResponse::Bytes { value }) => {
            let value = value.unwrap_or_default();
            let (start, stop) = normalize_range(start, stop, value.len());
            RespValue::Bulk(Some(value[start..stop].to_vec()))
        }
        Ok(_) => RespValue::Error("ERR invalid getrange response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn string_setrange_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let offset = match parse_usize(&args[2], "offset") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    match execute(Command::StringGet { key: key.clone() }) {
        Ok(CommandResponse::Bytes { value }) => {
            let mut value = value.unwrap_or_default();
            let Some(end) = offset.checked_add(args[3].len()) else {
                return RespValue::Error("ERR string exceeds maximum allowed size".to_string());
            };
            if value.len() < end {
                value.resize(end, 0);
            }
            value[offset..end].copy_from_slice(&args[3]);
            let len = value.len() as i64;
            match execute(Command::StringSet { key, value }) {
                Ok(_) => RespValue::Integer(len),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        Ok(_) => RespValue::Error("ERR invalid setrange response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn zrange_response(
    args: &[Vec<u8>],
    reverse: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let start = match parse_i64_arg(&args[2], "start") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let stop = match parse_i64_arg(&args[3], "stop") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let with_scores = args
        .get(4)
        .is_some_and(|arg| upper(arg).as_str() == "WITHSCORES");
    match load_redis_zset(&string_arg(&args[1]), execute) {
        Ok(mut values) => {
            sort_zset_values(&mut values);
            if reverse {
                values.reverse();
            }
            let (start, stop) = normalize_range(start, stop, values.len());
            let selected = &values[start..stop];
            if with_scores {
                RespValue::Array(
                    selected
                        .iter()
                        .flat_map(|(member, score)| {
                            [
                                RespValue::Bulk(Some(member.clone())),
                                RespValue::Bulk(Some(format_redis_score(*score).into_bytes())),
                            ]
                        })
                        .collect(),
                )
            } else {
                RespValue::Array(
                    selected
                        .iter()
                        .map(|(member, _)| RespValue::Bulk(Some(member.clone())))
                        .collect(),
                )
            }
        }
        Err(err) => RespValue::Error(err),
    }
}

fn zrank_response(
    args: &[Vec<u8>],
    reverse: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    match load_redis_zset(&string_arg(&args[1]), execute) {
        Ok(mut values) => {
            sort_zset_values(&mut values);
            if reverse {
                values.reverse();
            }
            values
                .iter()
                .position(|(member, _)| member == &args[2])
                .map(|rank| RespValue::Integer(rank as i64))
                .unwrap_or(RespValue::Bulk(None))
        }
        Err(err) => RespValue::Error(err),
    }
}

fn zincrby_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let increment = match parse_f64_arg(&args[2], "increment") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    let mut values = match load_redis_zset(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    let next_score =
        if let Some((_, score)) = values.iter_mut().find(|(member, _)| member == &args[3]) {
            *score += increment;
            *score
        } else {
            values.push((args[3].clone(), increment));
            increment
        };
    match store_redis_zset(&key, &values, execute) {
        Ok(()) => RespValue::Bulk(Some(format_redis_score(next_score).into_bytes())),
        Err(err) => RespValue::Error(err),
    }
}

fn zrange_by_score_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let min = match parse_f64_arg(&args[2], "min") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let max = match parse_f64_arg(&args[3], "max") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let with_scores = args
        .get(4)
        .is_some_and(|arg| upper(arg).as_str() == "WITHSCORES");
    match load_redis_zset(&string_arg(&args[1]), execute) {
        Ok(mut values) => {
            sort_zset_values(&mut values);
            let selected = values
                .into_iter()
                .filter(|(_, score)| *score >= min && *score <= max);
            if with_scores {
                RespValue::Array(
                    selected
                        .flat_map(|(member, score)| {
                            [
                                RespValue::Bulk(Some(member)),
                                RespValue::Bulk(Some(format_redis_score(score).into_bytes())),
                            ]
                        })
                        .collect(),
                )
            } else {
                RespValue::Array(
                    selected
                        .map(|(member, _)| RespValue::Bulk(Some(member)))
                        .collect(),
                )
            }
        }
        Err(err) => RespValue::Error(err),
    }
}

fn zpop_response(
    args: &[Vec<u8>],
    reverse: bool,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(&args[1]);
    let count = match args.get(2) {
        Some(value) => match parse_usize(value, "count") {
            Ok(value) => value,
            Err(err) => return RespValue::Error(err),
        },
        None => 1,
    };
    let mut values = match load_redis_zset(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    sort_zset_values(&mut values);
    if reverse {
        values.reverse();
    }
    let selected = values.iter().take(count).cloned().collect::<Vec<_>>();
    values.retain(|(member, _)| !selected.iter().any(|(selected, _)| selected == member));
    match store_redis_zset(&key, &values, execute) {
        Ok(()) => RespValue::Array(
            selected
                .into_iter()
                .flat_map(|(member, score)| {
                    [
                        RespValue::Bulk(Some(member)),
                        RespValue::Bulk(Some(format_redis_score(score).into_bytes())),
                    ]
                })
                .collect(),
        ),
        Err(err) => RespValue::Error(err),
    }
}

fn zremrangebyscore_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let min = match parse_f64_arg(&args[2], "min") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let max = match parse_f64_arg(&args[3], "max") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    let mut values = match load_redis_zset(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    let before = values.len();
    values.retain(|(_, score)| *score < min || *score > max);
    let removed = before.saturating_sub(values.len()) as i64;
    match store_redis_zset(&key, &values, execute) {
        Ok(()) => RespValue::Integer(removed),
        Err(err) => RespValue::Error(err),
    }
}

fn zremrangebyrank_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let start = match parse_i64_arg(&args[2], "start") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let stop = match parse_i64_arg(&args[3], "stop") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let key = string_arg(&args[1]);
    let mut values = match load_redis_zset(&key, execute) {
        Ok(values) => values,
        Err(err) => return RespValue::Error(err),
    };
    sort_zset_values(&mut values);
    let (start, stop) = normalize_range(start, stop, values.len());
    let removed_members = values[start..stop]
        .iter()
        .map(|(member, _)| member.clone())
        .collect::<HashSet<_>>();
    let removed = removed_members.len() as i64;
    values.retain(|(member, _)| !removed_members.contains(member));
    match store_redis_zset(&key, &values, execute) {
        Ok(()) => RespValue::Integer(removed),
        Err(err) => RespValue::Error(err),
    }
}

fn zrandmember_response(
    args: &[Vec<u8>],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let count = match args.get(2) {
        Some(value) => match parse_i64_arg(value, "count") {
            Ok(value) => Some(value),
            Err(err) => return RespValue::Error(err),
        },
        None => None,
    };
    let with_scores = args
        .get(3)
        .is_some_and(|arg| upper(arg).as_str() == "WITHSCORES");
    match load_redis_zset(&string_arg(&args[1]), execute) {
        Ok(mut values) => {
            sort_zset_values(&mut values);
            match count {
                Some(count) => {
                    let take = count.unsigned_abs() as usize;
                    let mut selected = Vec::new();
                    for index in 0..take {
                        if values.is_empty() {
                            break;
                        }
                        let entry = if count >= 0 {
                            values.get(index).cloned()
                        } else {
                            values.get(index % values.len()).cloned()
                        };
                        if let Some((member, score)) = entry {
                            selected.push(RespValue::Bulk(Some(member)));
                            if with_scores {
                                selected.push(RespValue::Bulk(Some(
                                    format_redis_score(score).into_bytes(),
                                )));
                            }
                        }
                    }
                    RespValue::Array(selected)
                }
                None => values
                    .into_iter()
                    .next()
                    .map(|(member, _)| RespValue::Bulk(Some(member)))
                    .unwrap_or(RespValue::Bulk(None)),
            }
        }
        Err(err) => RespValue::Error(err),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZSetAlgebraOp {
    Diff,
    Inter,
    Union,
}

fn zset_algebra_response(
    args: &[Vec<u8>],
    op: ZSetAlgebraOp,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key_count = match parse_usize(&args[1], "numkeys") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    if key_count == 0 || args.len() < key_count + 2 {
        return RespValue::Error("ERR syntax error".to_string());
    }
    let with_scores = args
        .get(key_count + 2)
        .is_some_and(|arg| upper(arg).as_str() == "WITHSCORES");
    let mut sets = Vec::new();
    for key in args.iter().skip(2).take(key_count) {
        match load_redis_zset(&string_arg(key), execute) {
            Ok(values) => sets.push(values.into_iter().collect::<HashMap<_, _>>()),
            Err(err) => return RespValue::Error(err),
        }
    }
    let mut result = match op {
        ZSetAlgebraOp::Diff | ZSetAlgebraOp::Inter => sets.first().cloned().unwrap_or_default(),
        ZSetAlgebraOp::Union => HashMap::new(),
    };
    match op {
        ZSetAlgebraOp::Diff => {
            for set in sets.iter().skip(1) {
                result.retain(|member, _| !set.contains_key(member));
            }
        }
        ZSetAlgebraOp::Inter => {
            for set in sets.iter().skip(1) {
                result.retain(|member, score| {
                    if let Some(other_score) = set.get(member) {
                        *score += *other_score;
                        true
                    } else {
                        false
                    }
                });
            }
        }
        ZSetAlgebraOp::Union => {
            for set in sets {
                for (member, score) in set {
                    *result.entry(member).or_insert(0.0) += score;
                }
            }
        }
    }
    let mut values = result.into_iter().collect::<Vec<_>>();
    sort_zset_values(&mut values);
    if with_scores {
        RespValue::Array(
            values
                .into_iter()
                .flat_map(|(member, score)| {
                    [
                        RespValue::Bulk(Some(member)),
                        RespValue::Bulk(Some(format_redis_score(score).into_bytes())),
                    ]
                })
                .collect(),
        )
    } else {
        RespValue::Array(
            values
                .into_iter()
                .map(|(member, _)| RespValue::Bulk(Some(member)))
                .collect(),
        )
    }
}

fn redis_keys_response(pattern: &[u8], state: &RedisCommandState) -> RespValue {
    let pattern = string_arg(pattern);
    RespValue::Array(
        sorted_matching_keys(&pattern, state)
            .into_iter()
            .map(|key| RespValue::Bulk(Some(key.into_bytes())))
            .collect(),
    )
}

fn redis_scan_response(args: &[Vec<u8>], state: &RedisCommandState) -> RespValue {
    let cursor = match parse_usize(&args[1], "cursor") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let mut pattern = "*".to_string();
    let mut count = 10usize;
    let mut index = 2;
    while index < args.len() {
        match upper(&args[index]).as_str() {
            "MATCH" => {
                let Some(value) = args.get(index + 1) else {
                    return RespValue::Error("ERR syntax error".to_string());
                };
                pattern = string_arg(value);
                index += 2;
            }
            "COUNT" => {
                let Some(value) = args.get(index + 1) else {
                    return RespValue::Error("ERR syntax error".to_string());
                };
                count = match parse_usize(value, "count") {
                    Ok(value) => value.max(1),
                    Err(err) => return RespValue::Error(err),
                };
                index += 2;
            }
            _ => return RespValue::Error("ERR syntax error".to_string()),
        }
    }
    let keys = sorted_matching_keys(&pattern, state);
    let selected = keys
        .iter()
        .skip(cursor)
        .take(count)
        .cloned()
        .collect::<Vec<_>>();
    let next_cursor = cursor.saturating_add(selected.len());
    let next_cursor = if next_cursor >= keys.len() {
        0
    } else {
        next_cursor
    };
    RespValue::Array(vec![
        RespValue::Bulk(Some(next_cursor.to_string().into_bytes())),
        RespValue::Array(
            selected
                .into_iter()
                .map(|key| RespValue::Bulk(Some(key.into_bytes())))
                .collect(),
        ),
    ])
}

fn parse_scan_tail_options(args: &[Vec<u8>]) -> Result<(String, usize), String> {
    let mut pattern = "*".to_string();
    let mut count = 10usize;
    let mut index = 0;
    while index < args.len() {
        match upper(&args[index]).as_str() {
            "MATCH" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                pattern = string_arg(value);
                index += 2;
            }
            "COUNT" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                count = parse_usize(value, "count")?.max(1);
                index += 2;
            }
            _ => return Err("ERR syntax error".to_string()),
        }
    }
    Ok((pattern, count))
}

fn redis_cursor_page_response(cursor: usize, count: usize, values: Vec<RespValue>) -> RespValue {
    let selected = values
        .iter()
        .skip(cursor)
        .take(count)
        .cloned()
        .collect::<Vec<_>>();
    let next_cursor = cursor.saturating_add(selected.len());
    let next_cursor = if next_cursor >= values.len() {
        0
    } else {
        next_cursor
    };
    RespValue::Array(vec![
        RespValue::Bulk(Some(next_cursor.to_string().into_bytes())),
        RespValue::Array(selected),
    ])
}

fn sorted_matching_keys(pattern: &str, state: &RedisCommandState) -> Vec<String> {
    let mut keys = state
        .keyspace
        .iter()
        .filter(|key| redis_pattern_matches(pattern, key))
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys
}

fn redis_pattern_matches(pattern: &str, value: &str) -> bool {
    fn matches_parts(pattern: &[u8], value: &[u8]) -> bool {
        match pattern.split_first() {
            None => value.is_empty(),
            Some((&b'*', rest)) => {
                matches_parts(rest, value)
                    || (!value.is_empty() && matches_parts(pattern, &value[1..]))
            }
            Some((&b'?', rest)) => !value.is_empty() && matches_parts(rest, &value[1..]),
            Some((&expected, rest)) => value.split_first().is_some_and(|(&actual, value_rest)| {
                actual == expected && matches_parts(rest, value_rest)
            }),
        }
    }
    matches_parts(pattern.as_bytes(), value.as_bytes())
}

fn redis_type_response(
    key: &[u8],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let key = string_arg(key);
    match execute(Command::StringGet { key: key.clone() }) {
        Ok(CommandResponse::Bytes { value: Some(value) }) => {
            let redis_type = if value.starts_with(REDIS_LIST_ENCODING_PREFIX) {
                "list"
            } else if value.starts_with(REDIS_ZSET_ENCODING_PREFIX) {
                "zset"
            } else {
                "string"
            };
            return RespValue::SimpleString(redis_type.to_string());
        }
        Ok(CommandResponse::Bytes { value: None }) => {}
        Ok(_) => return RespValue::Error("ERR invalid type string response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    }
    match execute(Command::HashLen { key: key.clone() }) {
        Ok(CommandResponse::Integer { value }) if value > 0 => {
            return RespValue::SimpleString("hash".to_string());
        }
        Ok(CommandResponse::Integer { .. }) => {}
        Ok(_) => return RespValue::Error("ERR invalid type hash response".to_string()),
        Err(err) => return RespValue::Error(format!("ERR {err}")),
    }
    match execute(Command::SetMembers { key }) {
        Ok(CommandResponse::Members { members }) if !members.is_empty() => {
            RespValue::SimpleString("set".to_string())
        }
        Ok(CommandResponse::Members { .. }) => RespValue::SimpleString("none".to_string()),
        Ok(_) => RespValue::Error("ERR invalid type set response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

fn load_redis_zset(
    key: &str,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Vec<(Vec<u8>, f64)>, String> {
    match execute(Command::StringGet {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Bytes { value: None }) => Ok(Vec::new()),
        Ok(CommandResponse::Bytes { value: Some(value) }) => decode_redis_zset(&value),
        Ok(_) => Err("ERR invalid zset backing response".to_string()),
        Err(err) => Err(format!("ERR {err}")),
    }
}

fn store_redis_zset(
    key: &str,
    values: &[(Vec<u8>, f64)],
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<(), String> {
    let mut sorted = values.to_vec();
    sort_zset_values(&mut sorted);
    execute(Command::StringSet {
        key: key.to_string(),
        value: encode_redis_zset(&sorted),
    })
    .map(|_| ())
    .map_err(|err| format!("ERR {err}"))
}

fn encode_redis_zset(values: &[(Vec<u8>, f64)]) -> Vec<u8> {
    let mut out = REDIS_ZSET_ENCODING_PREFIX.to_vec();
    out.extend_from_slice(&(values.len() as u64).to_be_bytes());
    for (member, score) in values {
        out.extend_from_slice(&score.to_be_bytes());
        out.extend_from_slice(&(member.len() as u64).to_be_bytes());
        out.extend_from_slice(member);
    }
    out
}

fn decode_redis_zset(value: &[u8]) -> Result<Vec<(Vec<u8>, f64)>, String> {
    if !value.starts_with(REDIS_ZSET_ENCODING_PREFIX) {
        return Err(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        );
    }
    let mut offset = REDIS_ZSET_ENCODING_PREFIX.len();
    let count = read_u64_be(value, &mut offset)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let score = read_f64_be(value, &mut offset)?;
        let len = read_u64_be(value, &mut offset)? as usize;
        let Some(end) = offset.checked_add(len) else {
            return Err("ERR corrupt zset encoding".to_string());
        };
        if end > value.len() {
            return Err("ERR corrupt zset encoding".to_string());
        }
        out.push((value[offset..end].to_vec(), score));
        offset = end;
    }
    if offset != value.len() {
        return Err("ERR corrupt zset encoding".to_string());
    }
    Ok(out)
}

fn upsert_zset_member(values: &mut Vec<(Vec<u8>, f64)>, member: Vec<u8>, score: f64) -> bool {
    if let Some((_, existing_score)) = values.iter_mut().find(|(existing, _)| existing == &member) {
        *existing_score = score;
        false
    } else {
        values.push((member, score));
        true
    }
}

fn sort_zset_values(values: &mut [(Vec<u8>, f64)]) {
    values.sort_by(|(left_member, left_score), (right_member, right_score)| {
        left_score
            .total_cmp(right_score)
            .then_with(|| left_member.cmp(right_member))
    });
}

fn normalize_index(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let index = if index < 0 { len + index } else { index };
    if index < 0 || index >= len {
        None
    } else {
        Some(index as usize)
    }
}

fn normalize_range(start: i64, stop: i64, len: usize) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let len_i64 = len as i64;
    let mut start = if start < 0 { len_i64 + start } else { start };
    let mut stop = if stop < 0 { len_i64 + stop } else { stop };
    if start < 0 {
        start = 0;
    }
    if stop < 0 || start >= len_i64 {
        return (0, 0);
    }
    if stop >= len_i64 {
        stop = len_i64 - 1;
    }
    if start > stop {
        (0, 0)
    } else {
        (start as usize, stop as usize + 1)
    }
}

fn read_u64_be(value: &[u8], offset: &mut usize) -> Result<u64, String> {
    let Some(end) = offset.checked_add(8) else {
        return Err("ERR corrupt redis compatibility encoding".to_string());
    };
    let bytes = value
        .get(*offset..end)
        .ok_or_else(|| "ERR corrupt redis compatibility encoding".to_string())?;
    *offset = end;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("slice length checked"),
    ))
}

fn read_f64_be(value: &[u8], offset: &mut usize) -> Result<f64, String> {
    let Some(end) = offset.checked_add(8) else {
        return Err("ERR corrupt zset encoding".to_string());
    };
    let bytes = value
        .get(*offset..end)
        .ok_or_else(|| "ERR corrupt zset encoding".to_string())?;
    *offset = end;
    Ok(f64::from_be_bytes(
        bytes.try_into().expect("slice length checked"),
    ))
}

fn format_redis_score(score: f64) -> String {
    if score.fract() == 0.0 {
        format!("{score:.0}")
    } else {
        score.to_string()
    }
}

fn parse_f64_arg(value: &[u8], name: &str) -> Result<f64, String> {
    let value = std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .ok_or_else(|| format!("ERR {name} must be a float"))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(format!("ERR {name} must be finite"))
    }
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
        "FIRST" | "NX" | "INSERT_IF_ABSENT" => Ok(FeatureWritePolicy::InsertIfAbsent),
        "UPDATE" | "XX" | "REPLACE_EXISTING" => Ok(FeatureWritePolicy::ReplaceExisting),
        "BLOCK" => Ok(FeatureWritePolicy::Block),
        _ => Err("ERR policy must be UPSERT, FIRST/NX, UPDATE/XX, or BLOCK".to_string()),
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

fn string_arg(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_string()
}

fn upper(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use std::io::BufReader;
    use std::sync::Mutex;

    use super::*;
    use crate::engine::TemporalEngine;
    use crate::types::{ExecuteRequest, SequenceFeatureRow};

    static OPEN_SOURCE_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn resp_parser_reads_array_command() {
        let mut input = BufReader::new(&b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"[..]);
        assert_eq!(
            read_command(&mut input).unwrap(),
            Some(vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()])
        );
    }

    #[test]
    fn redis_info_stats_report_command_and_rejection_counters() {
        let _guard = OPEN_SOURCE_ENV_LOCK.lock().unwrap();
        std::env::remove_var("TEMPORALSTORE_OPEN_SOURCE_SURFACE");

        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter()
                    .map(|arg| arg.as_bytes().to_vec())
                    .collect(),
                1,
                state,
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
            run(&mut state, vec!["PING"]),
            RespValue::SimpleString("PONG".to_string())
        );
        assert!(matches!(
            run(&mut state, vec!["NO_SUCH_TEMPORALSTORE_COMMAND"]),
            RespValue::Error(_)
        ));
        let info = run(&mut state, vec!["INFO", "stats"]);
        let RespValue::Bulk(Some(info)) = info else {
            panic!("INFO stats must return bulk string");
        };
        let info = String::from_utf8(info).unwrap();
        assert!(info.contains("total_commands_processed:3"), "{info}");
        assert!(info.contains("rejected_commands:1"), "{info}");
        assert!(info.contains("unsupported_commands:1"), "{info}");
    }

    #[test]
    fn redis_open_source_surface_is_trimmed_to_production_data_models() {
        let _guard = OPEN_SOURCE_ENV_LOCK.lock().unwrap();
        std::env::set_var("TEMPORALSTORE_OPEN_SOURCE_SURFACE", "1");

        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter()
                    .map(|arg| arg.as_bytes().to_vec())
                    .collect(),
                1,
                state,
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

        let advertised = run(&mut state, vec!["COMMAND"]);
        let RespValue::Array(commands) = advertised else {
            panic!("COMMAND must return an array");
        };
        let command_names = commands
            .into_iter()
            .map(|value| match value {
                RespValue::Bulk(Some(bytes)) => String::from_utf8(bytes).unwrap(),
                other => panic!("unexpected COMMAND entry: {other:?}"),
            })
            .collect::<Vec<_>>();
        let command_count = run(&mut state, vec!["COMMAND", "COUNT"]);
        assert_eq!(
            command_count,
            RespValue::Integer(command_names.len() as i64)
        );
        for allowed in [
            "CLIENT", "QUIT", "GET", "SET", "HSET", "HGET", "FAPPEND", "FQUERY", "RISKINCR",
            "CPCSET", "FOLQUERY",
        ] {
            assert!(
                command_names.contains(&allowed.to_string()),
                "missing {allowed}"
            );
        }
        for denied in [
            "SADD",
            "LPUSH",
            "ZADD",
            "SCAN",
            "PARTITION",
            "FADD",
            "IPSADD",
            "RISKDEBUG",
        ] {
            assert!(
                !command_names.contains(&denied.to_string()),
                "unexpected {denied}"
            );
        }

        assert_eq!(
            run(&mut state, vec!["FAPPEND", "feature", "10", "2"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["RISKINCR", "risk", "10", "5"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["CLIENT", "SETNAME", "matrixark-smoke"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["CLIENT", "GETNAME"]),
            RespValue::Bulk(None)
        );
        assert_eq!(
            run(&mut state, vec!["QUIT"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert!(matches!(
            run(&mut state, vec!["LPUSH", "list", "v"]),
            RespValue::Error(message) if message.contains("open-source Redis surface")
        ));
        assert!(matches!(
            run(&mut state, vec!["FADD", "feature", "10", "2"]),
            RespValue::Error(message) if message.contains("open-source Redis surface")
        ));
        let info = run(&mut state, vec!["INFO", "stats"]);
        let RespValue::Bulk(Some(info)) = info else {
            panic!("INFO stats must return bulk string");
        };
        let info = String::from_utf8(info).unwrap();
        assert!(info.contains("rejected_commands:2"), "{info}");
        assert!(info.contains("open_source_rejected_commands:2"), "{info}");
        assert!(info.contains("unsupported_commands:0"), "{info}");

        std::env::remove_var("TEMPORALSTORE_OPEN_SOURCE_SURFACE");
    }

    #[test]
    // shared-corpus: redis_engine_product_command_flow;
    fn redis_core_api_extensions_use_engine_and_state() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter()
                    .map(|arg| arg.as_bytes().to_vec())
                    .collect(),
                1,
                state,
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

        assert_eq!(run(&mut state, vec!["DBSIZE"]), RespValue::Integer(0));
        assert_eq!(
            run(&mut state, vec!["SET", "redis:core:string", "value"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["GETRANGE", "redis:core:string", "0", "4"]),
            RespValue::Bulk(Some(b"value".to_vec()))
        );
        assert_eq!(
            run(
                &mut state,
                vec!["SETRANGE", "redis:core:string", "5", "-tail"]
            ),
            RespValue::Integer(10)
        );
        assert_eq!(
            run(&mut state, vec!["GETRANGE", "redis:core:string", "0", "-1"]),
            RespValue::Bulk(Some(b"value-tail".to_vec()))
        );
        assert_eq!(
            run(
                &mut state,
                vec!["GETEX", "redis:core:string", "PX", "60000"]
            ),
            RespValue::Bulk(Some(b"value-tail".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["INCRBYFLOAT", "redis:core:float", "1.5"]),
            RespValue::Bulk(Some(b"1.5".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["TYPE", "redis:core:string"]),
            RespValue::SimpleString("string".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["HSETNX", "redis:core:hash", "field", "v1"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, vec!["HSETNX", "redis:core:hash", "field", "v2"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(&mut state, vec!["HEXISTS", "redis:core:hash", "field"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, vec!["HEXISTS", "redis:core:hash", "missing"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(&mut state, vec!["HKEYS", "redis:core:hash"]),
            RespValue::Array(vec![RespValue::Bulk(Some(b"field".to_vec()))])
        );
        assert_eq!(
            run(&mut state, vec!["HVALS", "redis:core:hash"]),
            RespValue::Array(vec![RespValue::Bulk(Some(b"v1".to_vec()))])
        );
        assert_eq!(
            run(&mut state, vec!["HSTRLEN", "redis:core:hash", "field"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(
                &mut state,
                vec!["HINCRBYFLOAT", "redis:core:hash", "float", "1.5"]
            ),
            RespValue::Bulk(Some(b"1.5".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["HSCAN", "redis:core:hash", "0"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"0".to_vec())),
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"field".to_vec())),
                    RespValue::Bulk(Some(b"v1".to_vec())),
                    RespValue::Bulk(Some(b"float".to_vec())),
                    RespValue::Bulk(Some(b"1.5".to_vec())),
                ]),
            ])
        );
        assert_eq!(
            run(&mut state, vec!["TYPE", "redis:core:hash"]),
            RespValue::SimpleString("hash".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["SADD", "redis:core:set", "a", "b"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(&mut state, vec!["SADD", "redis:core:other-set", "b", "c"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(
                &mut state,
                vec!["SDIFF", "redis:core:set", "redis:core:other-set"]
            ),
            RespValue::Array(vec![RespValue::Bulk(Some(b"a".to_vec()))])
        );
        assert_eq!(
            run(
                &mut state,
                vec!["SINTER", "redis:core:set", "redis:core:other-set"]
            ),
            RespValue::Array(vec![RespValue::Bulk(Some(b"b".to_vec()))])
        );
        assert_eq!(
            run(
                &mut state,
                vec!["SUNION", "redis:core:set", "redis:core:other-set"]
            ),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"a".to_vec())),
                RespValue::Bulk(Some(b"b".to_vec())),
                RespValue::Bulk(Some(b"c".to_vec())),
            ])
        );
        assert_eq!(
            run(&mut state, vec!["SRANDMEMBER", "redis:core:set"]),
            RespValue::Bulk(Some(b"a".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["SSCAN", "redis:core:set", "0"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"0".to_vec())),
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"a".to_vec())),
                    RespValue::Bulk(Some(b"b".to_vec())),
                ]),
            ])
        );
        assert_eq!(
            run(
                &mut state,
                vec!["SMOVE", "redis:core:set", "redis:core:set-dest", "a"]
            ),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, vec!["SPOP", "redis:core:set-dest"]),
            RespValue::Bulk(Some(b"a".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["TYPE", "redis:core:set"]),
            RespValue::SimpleString("set".to_string())
        );
        assert_eq!(
            run(
                &mut state,
                vec!["RPUSH", "redis:core:list", "a", "b", "a", "c", "a"]
            ),
            RespValue::Integer(5)
        );
        assert_eq!(
            run(&mut state, vec!["LREM", "redis:core:list", "2", "a"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(&mut state, vec!["LRANGE", "redis:core:list", "0", "-1"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"b".to_vec())),
                RespValue::Bulk(Some(b"c".to_vec())),
                RespValue::Bulk(Some(b"a".to_vec())),
            ])
        );
        assert_eq!(
            run(
                &mut state,
                vec!["LINSERT", "redis:core:list", "BEFORE", "c", "x"]
            ),
            RespValue::Integer(4)
        );
        assert_eq!(
            run(&mut state, vec!["LPOS", "redis:core:list", "a"]),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(
                &mut state,
                vec![
                    "LMOVE",
                    "redis:core:list",
                    "redis:core:list2",
                    "RIGHT",
                    "LEFT"
                ]
            ),
            RespValue::Bulk(Some(b"a".to_vec()))
        );
        assert_eq!(
            run(
                &mut state,
                vec!["RPOPLPUSH", "redis:core:list2", "redis:core:list"]
            ),
            RespValue::Bulk(Some(b"a".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["TYPE", "redis:core:list"]),
            RespValue::SimpleString("list".to_string())
        );
        assert_eq!(
            run(
                &mut state,
                vec!["ZADD", "redis:core:zset", "1", "a", "2", "b", "3", "c"]
            ),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(
                &mut state,
                vec!["ZRANGEBYSCORE", "redis:core:zset", "1.5", "3", "WITHSCORES"]
            ),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"b".to_vec())),
                RespValue::Bulk(Some(b"2".to_vec())),
                RespValue::Bulk(Some(b"c".to_vec())),
                RespValue::Bulk(Some(b"3".to_vec())),
            ])
        );
        assert_eq!(
            run(&mut state, vec!["ZINCRBY", "redis:core:zset", "2", "b"]),
            RespValue::Bulk(Some(b"4".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["ZRANK", "redis:core:zset", "c"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, vec!["ZREVRANK", "redis:core:zset", "b"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(
                &mut state,
                vec!["ZMSCORE", "redis:core:zset", "a", "missing"]
            ),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"1".to_vec())),
                RespValue::Bulk(None),
            ])
        );
        assert_eq!(
            run(&mut state, vec!["ZRANDMEMBER", "redis:core:zset"]),
            RespValue::Bulk(Some(b"a".to_vec()))
        );
        assert_eq!(
            run(
                &mut state,
                vec!["ZADD", "redis:core:zset2", "2", "a", "5", "d"]
            ),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(
                &mut state,
                vec!["ZDIFF", "2", "redis:core:zset", "redis:core:zset2"]
            ),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"c".to_vec())),
                RespValue::Bulk(Some(b"b".to_vec())),
            ])
        );
        assert_eq!(
            run(
                &mut state,
                vec![
                    "ZINTER",
                    "2",
                    "redis:core:zset",
                    "redis:core:zset2",
                    "WITHSCORES"
                ]
            ),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"a".to_vec())),
                RespValue::Bulk(Some(b"3".to_vec())),
            ])
        );
        assert_eq!(
            run(
                &mut state,
                vec!["ZUNION", "2", "redis:core:zset", "redis:core:zset2"]
            ),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"a".to_vec())),
                RespValue::Bulk(Some(b"c".to_vec())),
                RespValue::Bulk(Some(b"b".to_vec())),
                RespValue::Bulk(Some(b"d".to_vec())),
            ])
        );
        assert_eq!(
            run(
                &mut state,
                vec!["ZSCAN", "redis:core:zset", "0", "COUNT", "4"]
            ),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"4".to_vec())),
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"a".to_vec())),
                    RespValue::Bulk(Some(b"1".to_vec())),
                    RespValue::Bulk(Some(b"c".to_vec())),
                    RespValue::Bulk(Some(b"3".to_vec())),
                ]),
            ])
        );
        assert_eq!(
            run(&mut state, vec!["ZPOPMIN", "redis:core:zset"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"a".to_vec())),
                RespValue::Bulk(Some(b"1".to_vec())),
            ])
        );
        assert_eq!(
            run(&mut state, vec!["ZPOPMAX", "redis:core:zset"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"b".to_vec())),
                RespValue::Bulk(Some(b"4".to_vec())),
            ])
        );
        assert_eq!(
            run(
                &mut state,
                vec!["ZREMRANGEBYSCORE", "redis:core:zset", "3", "3"]
            ),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(
                &mut state,
                vec!["ZREMRANGEBYRANK", "redis:core:zset", "0", "0"]
            ),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(&mut state, vec!["TYPE", "redis:core:zset"]),
            RespValue::SimpleString("zset".to_string())
        );
        assert_eq!(
            run(
                &mut state,
                vec!["COPY", "redis:core:string", "redis:core:string-copy"]
            ),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(
                &mut state,
                vec![
                    "RENAMENX",
                    "redis:core:string-copy",
                    "redis:core:string-renamed"
                ]
            ),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["GET", "redis:core:string-renamed"]),
            RespValue::Bulk(Some(b"value-tail".to_vec()))
        );
        assert_eq!(
            run(
                &mut state,
                vec![
                    "RENAME",
                    "redis:core:string-renamed",
                    "redis:core:string-copy"
                ]
            ),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["RANDOMKEY"]),
            RespValue::Bulk(Some(b"redis:core:float".to_vec()))
        );
        assert_eq!(
            run(
                &mut state,
                vec!["TOUCH", "redis:core:string", "redis:core:missing"]
            ),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, vec!["KEYS", "redis:core:*"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"redis:core:float".to_vec())),
                RespValue::Bulk(Some(b"redis:core:hash".to_vec())),
                RespValue::Bulk(Some(b"redis:core:list".to_vec())),
                RespValue::Bulk(Some(b"redis:core:list2".to_vec())),
                RespValue::Bulk(Some(b"redis:core:other-set".to_vec())),
                RespValue::Bulk(Some(b"redis:core:set".to_vec())),
                RespValue::Bulk(Some(b"redis:core:set-dest".to_vec())),
                RespValue::Bulk(Some(b"redis:core:string".to_vec())),
                RespValue::Bulk(Some(b"redis:core:string-copy".to_vec())),
                RespValue::Bulk(Some(b"redis:core:zset".to_vec())),
                RespValue::Bulk(Some(b"redis:core:zset2".to_vec())),
            ])
        );
        assert_eq!(
            run(
                &mut state,
                vec!["SCAN", "0", "MATCH", "redis:core:*", "COUNT", "2"]
            ),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"2".to_vec())),
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"redis:core:float".to_vec())),
                    RespValue::Bulk(Some(b"redis:core:hash".to_vec())),
                ]),
            ])
        );
        assert_eq!(
            run(&mut state, vec!["TYPE", "redis:core:missing"]),
            RespValue::SimpleString("none".to_string())
        );
        assert_eq!(run(&mut state, vec!["DBSIZE"]), RespValue::Integer(11));
        assert_eq!(
            run(&mut state, vec!["DEL", "redis:core:string"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, vec!["UNLINK", "redis:core:other-set"]),
            RespValue::Integer(1)
        );
        assert_eq!(run(&mut state, vec!["DBSIZE"]), RespValue::Integer(9));
        assert_eq!(
            run(&mut state, vec!["FLUSHDB"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(run(&mut state, vec!["DBSIZE"]), RespValue::Integer(0));
    }

    #[test]
    // shared-corpus: redis_engine_product_command_flow;
    fn advertised_redis_commands_have_dispatch_paths() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        for descriptor in redis_supported_commands() {
            let args = sample_args_for_command(descriptor.name);
            let response = execute_redis_command_with_state(
                args.iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                &mut state,
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
            );
            assert_ne!(
                response,
                RespValue::Error("ERR syntax error".to_string()),
                "{} is advertised by COMMAND but has no dispatch path",
                descriptor.name
            );
        }
    }

    fn sample_args_for_command(command: &str) -> Vec<&'static str> {
        match command {
            "APPEND" => vec!["APPEND", "advertised:append", "x"],
            "AUTH" => vec!["AUTH", ""],
            "BGSAVE" => vec!["BGSAVE"],
            "CLIENT" => vec!["CLIENT", "SETNAME", "matrixark-smoke"],
            "COMMAND" => vec!["COMMAND", "COUNT"],
            "CONFIG" => vec!["CONFIG", "GET", "maxmemory"],
            "COPY" => vec!["COPY", "advertised:missing", "advertised:copy"],
            "DBSIZE" => vec!["DBSIZE"],
            "DEL" => vec!["DEL", "advertised:missing"],
            "ECHO" => vec!["ECHO", "hello"],
            "EXISTS" => vec!["EXISTS", "advertised:missing"],
            "EXPIRE" => vec!["EXPIRE", "advertised:missing", "10"],
            "EXPIREAT" => vec!["EXPIREAT", "advertised:missing", "4102444800"],
            "EXPIRETIME" => vec!["EXPIRETIME", "advertised:missing"],
            "FLUSHALL" => vec!["FLUSHALL"],
            "FLUSHDB" => vec!["FLUSHDB"],
            "GET" => vec!["GET", "advertised:missing"],
            "GETDEL" => vec!["GETDEL", "advertised:missing"],
            "GETEX" => vec!["GETEX", "advertised:missing"],
            "GETRANGE" => vec!["GETRANGE", "advertised:missing", "0", "-1"],
            "GETSET" => vec!["GETSET", "advertised:getset", "v"],
            "HDEL" => vec!["HDEL", "advertised:hash", "missing"],
            "HEXISTS" => vec!["HEXISTS", "advertised:hash", "missing"],
            "HGET" => vec!["HGET", "advertised:hash", "missing"],
            "HGETALL" => vec!["HGETALL", "advertised:hash"],
            "HINCRBY" => vec!["HINCRBY", "advertised:hash", "n", "1"],
            "HINCRBYFLOAT" => vec!["HINCRBYFLOAT", "advertised:hash", "f", "1.5"],
            "HKEYS" => vec!["HKEYS", "advertised:hash"],
            "HLEN" => vec!["HLEN", "advertised:hash"],
            "HMGET" => vec!["HMGET", "advertised:hash", "missing"],
            "HMSET" => vec!["HMSET", "advertised:hash", "a", "1"],
            "HSET" => vec!["HSET", "advertised:hash", "b", "2"],
            "HSCAN" => vec!["HSCAN", "advertised:hash", "0"],
            "HSETNX" => vec!["HSETNX", "advertised:hash", "c", "3"],
            "HSTRLEN" => vec!["HSTRLEN", "advertised:hash", "a"],
            "HVALS" => vec!["HVALS", "advertised:hash"],
            "INFO" => vec!["INFO"],
            "INCRBYFLOAT" => vec!["INCRBYFLOAT", "advertised:float", "1.5"],
            "KEYS" => vec!["KEYS", "*"],
            "LINSERT" => vec!["LINSERT", "advertised:list", "BEFORE", "a", "b"],
            "LINDEX" => vec!["LINDEX", "advertised:list", "0"],
            "LLEN" => vec!["LLEN", "advertised:list"],
            "LMOVE" => vec![
                "LMOVE",
                "advertised:list",
                "advertised:list2",
                "RIGHT",
                "LEFT",
            ],
            "LPOP" => vec!["LPOP", "advertised:list"],
            "LPUSH" => vec!["LPUSH", "advertised:list", "a"],
            "LPOS" => vec!["LPOS", "advertised:list", "a"],
            "LREM" => vec!["LREM", "advertised:list", "0", "a"],
            "LRANGE" => vec!["LRANGE", "advertised:list", "0", "-1"],
            "LSET" => vec!["LSET", "advertised:list", "0", "b"],
            "LTRIM" => vec!["LTRIM", "advertised:list", "0", "-1"],
            "MGET" => vec!["MGET", "advertised:missing"],
            "MSET" => vec!["MSET", "advertised:mset", "v"],
            "MSETNX" => vec!["MSETNX", "advertised:msetnx", "v"],
            "PARTITION" => vec!["PARTITION", "INFO"],
            "PEXPIRE" => vec!["PEXPIRE", "advertised:missing", "10"],
            "PEXPIREAT" => vec!["PEXPIREAT", "advertised:missing", "4102444800000"],
            "PEXPIRETIME" => vec!["PEXPIRETIME", "advertised:missing"],
            "PING" => vec!["PING"],
            "PSETEX" => vec!["PSETEX", "advertised:psetex", "10", "v"],
            "PTTL" => vec!["PTTL", "advertised:missing"],
            "QUIT" => vec!["QUIT"],
            "RANDOMKEY" => vec!["RANDOMKEY"],
            "RENAME" => vec!["RENAME", "advertised:missing", "advertised:renamed"],
            "RENAMENX" => vec!["RENAMENX", "advertised:missing", "advertised:renamed"],
            "RPOP" => vec!["RPOP", "advertised:list"],
            "RPOPLPUSH" => vec!["RPOPLPUSH", "advertised:list", "advertised:list2"],
            "RPUSH" => vec!["RPUSH", "advertised:list", "z"],
            "SADD" => vec!["SADD", "advertised:set", "a"],
            "SCARD" => vec!["SCARD", "advertised:set"],
            "SDIFF" => vec!["SDIFF", "advertised:set", "advertised:other"],
            "SCAN" => vec!["SCAN", "0"],
            "SELECT" => vec!["SELECT", "0"],
            "SET" => vec!["SET", "advertised:set-string", "v"],
            "SETEX" => vec!["SETEX", "advertised:setex", "10", "v"],
            "SETNX" => vec!["SETNX", "advertised:setnx", "v"],
            "SETRANGE" => vec!["SETRANGE", "advertised:setrange", "0", "x"],
            "SISMEMBER" => vec!["SISMEMBER", "advertised:set", "a"],
            "SINTER" => vec!["SINTER", "advertised:set", "advertised:other"],
            "SLAVEOF" => vec!["SLAVEOF", "no", "one"],
            "SMEMBERS" => vec!["SMEMBERS", "advertised:set"],
            "SMISMEMBER" => vec!["SMISMEMBER", "advertised:set", "a"],
            "SMOVE" => vec!["SMOVE", "advertised:set", "advertised:other", "a"],
            "SPOP" => vec!["SPOP", "advertised:set"],
            "SRANDMEMBER" => vec!["SRANDMEMBER", "advertised:set"],
            "SREM" => vec!["SREM", "advertised:set", "a"],
            "SSCAN" => vec!["SSCAN", "advertised:set", "0"],
            "STRLEN" => vec!["STRLEN", "advertised:missing"],
            "SUNION" => vec!["SUNION", "advertised:set", "advertised:other"],
            "TTL" => vec!["TTL", "advertised:missing"],
            "TOUCH" => vec!["TOUCH", "advertised:missing"],
            "TYPE" => vec!["TYPE", "advertised:missing"],
            "UNLINK" => vec!["UNLINK", "advertised:missing"],
            "ZADD" => vec!["ZADD", "advertised:zset", "1", "a"],
            "ZCARD" => vec!["ZCARD", "advertised:zset"],
            "ZCOUNT" => vec!["ZCOUNT", "advertised:zset", "0", "10"],
            "ZDIFF" => vec!["ZDIFF", "1", "advertised:zset"],
            "ZINCRBY" => vec!["ZINCRBY", "advertised:zset", "1", "a"],
            "ZINTER" => vec!["ZINTER", "1", "advertised:zset"],
            "ZMSCORE" => vec!["ZMSCORE", "advertised:zset", "a"],
            "ZPOPMAX" => vec!["ZPOPMAX", "advertised:zset"],
            "ZPOPMIN" => vec!["ZPOPMIN", "advertised:zset"],
            "ZRANGE" => vec!["ZRANGE", "advertised:zset", "0", "-1"],
            "ZRANGEBYSCORE" => vec!["ZRANGEBYSCORE", "advertised:zset", "0", "10"],
            "ZRANK" => vec!["ZRANK", "advertised:zset", "a"],
            "ZRANDMEMBER" => vec!["ZRANDMEMBER", "advertised:zset"],
            "ZREM" => vec!["ZREM", "advertised:zset", "a"],
            "ZREMRANGEBYRANK" => vec!["ZREMRANGEBYRANK", "advertised:zset", "0", "1"],
            "ZREMRANGEBYSCORE" => vec!["ZREMRANGEBYSCORE", "advertised:zset", "0", "10"],
            "ZREVRANGE" => vec!["ZREVRANGE", "advertised:zset", "0", "-1"],
            "ZREVRANK" => vec!["ZREVRANK", "advertised:zset", "a"],
            "ZSCAN" => vec!["ZSCAN", "advertised:zset", "0"],
            "ZSCORE" => vec!["ZSCORE", "advertised:zset", "a"],
            "ZUNION" => vec!["ZUNION", "1", "advertised:zset"],
            other => panic!("missing sample command for {other}"),
        }
    }

    #[test]
    // shared-corpus: redis_engine_product_command_flow;
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
        assert_eq!(run(vec!["RPUSH", "list", "a", "b"]), RespValue::Integer(2));
        assert_eq!(run(vec!["LPUSH", "list", "left"]), RespValue::Integer(3));
        assert_eq!(
            run(vec!["LRANGE", "list", "0", "-1"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"left".to_vec())),
                RespValue::Bulk(Some(b"a".to_vec())),
                RespValue::Bulk(Some(b"b".to_vec())),
            ])
        );
        assert_eq!(
            run(vec!["LINDEX", "list", "-1"]),
            RespValue::Bulk(Some(b"b".to_vec()))
        );
        assert_eq!(
            run(vec!["LSET", "list", "1", "middle"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(vec!["LPOP", "list"]),
            RespValue::Bulk(Some(b"left".to_vec()))
        );
        assert_eq!(
            run(vec!["RPOP", "list", "2"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"middle".to_vec())),
                RespValue::Bulk(Some(b"b".to_vec())),
            ])
        );
        assert_eq!(run(vec!["LLEN", "list"]), RespValue::Integer(0));
        assert_eq!(
            run(vec!["ZADD", "z", "2", "b", "1", "a", "3.5", "c"]),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(vec!["ZRANGE", "z", "0", "-1", "WITHSCORES"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"a".to_vec())),
                RespValue::Bulk(Some(b"1".to_vec())),
                RespValue::Bulk(Some(b"b".to_vec())),
                RespValue::Bulk(Some(b"2".to_vec())),
                RespValue::Bulk(Some(b"c".to_vec())),
                RespValue::Bulk(Some(b"3.5".to_vec())),
            ])
        );
        assert_eq!(
            run(vec!["ZREVRANGE", "z", "0", "1"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"c".to_vec())),
                RespValue::Bulk(Some(b"b".to_vec())),
            ])
        );
        assert_eq!(
            run(vec!["ZSCORE", "z", "b"]),
            RespValue::Bulk(Some(b"2".to_vec()))
        );
        assert_eq!(
            run(vec!["ZCOUNT", "z", "1.5", "3.5"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(vec!["ZREM", "z", "b", "missing"]),
            RespValue::Integer(1)
        );
        assert_eq!(run(vec!["ZCARD", "z"]), RespValue::Integer(2));
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

    // shared-corpus: redis_engine_product_command_flow;
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
}
