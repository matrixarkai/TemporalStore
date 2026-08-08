//! Redis command dispatcher (execute_redis_command_with_state), extracted from redis.rs.

use super::*;
use crate::types::{Command, CommandResponse, FeatureFilterOp, FeatureWritePolicy, ControlStateFamily, StringSetCondition};

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
            RespValue::Integer(bucket_id_for_key(String::from_utf8_lossy(&args[1]).as_ref()) as i64)
        }
        "PCLUSTERKEYSLOT" if args.len() == 2 => {
            RespValue::Integer(bucket_id_for_key(String::from_utf8_lossy(&args[1]).as_ref()) as i64)
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
            // NOTE: C++'s redis bridge rejects TTL+NX/XX ("not supported ... yet"), but that
            // is a stopgap limitation, not a semantic -- real Redis supports SET k v NX EX 10
            // and Rust deliberately does too (see the redis_string_hash_set_and_feature test).
            // We keep the more-capable Rust behavior rather than match a C++ shim's TODO.
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
            // C++ rejects a non-positive expiry ("invalid expire time"); Rust otherwise
            // stored a key with deadline now+0 that vanishes on the next access.
            Ok(0) => RespValue::Error("ERR invalid expire time in setex".to_string()),
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
            Ok(0) => RespValue::Error("ERR invalid expire time in psetex".to_string()),
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
            // C++ TTL: the negative sentinels (-2 missing, -1 no-expiry) pass through
            // unchanged; a positive remaining-ms value rounds UP to seconds ((ms+999)/1000).
            // The old `value / 1000` turned -1/-2 into 0 and floored sub-second remainders.
            Ok(CommandResponse::Integer { value }) => {
                if value < 0 {
                    RespValue::Integer(value)
                } else {
                    RespValue::Integer((value + 999) / 1000)
                }
            }
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
            // C++ rejects DECRBY i64::MIN ("decrement would overflow"): negating it
            // overflows i64. Plain `-decrement` panics in debug and wraps to i64::MIN in
            // release (then silently stores a wrong value). Guard with checked_neg.
            Ok(decrement) => match decrement.checked_neg() {
                Some(neg) => {
                    let response = string_increment_response(&args[1], neg, &mut execute);
                    if matches!(response, RespValue::Integer(_)) {
                        state.keyspace.insert(string_arg(&args[1]));
                    }
                    response
                }
                None => RespValue::Error("ERR decrement would overflow".to_string()),
            },
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
                        // Negative count = |count| members WITH repetition. C++ computes
                        // n = -count; for i64::MIN that overflows to a negative n so the
                        // fill loop runs zero times. unsigned_abs() would instead yield
                        // 2^63 here and attempt an unbounded allocation (hang / OOM DoS).
                        let take = count.checked_neg().map(|n| n as usize).unwrap_or(0);
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
            let min = match parse_score_bound(&args[2], "min") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let max = match parse_score_bound(&args[3], "max") {
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
        "CONTROLSTATEINCR" if args.len() == 4 => {
            let timestamp_ms = match parse_u64(&args[2], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let amount = match parse_i64_arg(&args[3], "amount") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            status_ok(execute(Command::ControlStateIncrement {
                key: string_arg(&args[1]),
                timestamp_ms,
                amount,
            }))
        }
        "CONTROLSTATEINCROPT" if args.len() == 6 => {
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
            status_ok(execute(Command::ControlStateIncrementWithOptions {
                key: string_arg(&args[1]),
                timestamp_ms,
                amount,
                precision_ms: Some(precision_ms),
                ttl_ms: Some(ttl_ms),
            }))
        }
        "CONTROLSTATECHANGE" | "HCHANGE" if args.len() == 4 || args.len() == 6 => {
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
                control_state_family_key_for_resp(ControlStateFamily::H, &string_arg(&args[1]))
            } else {
                string_arg(&args[1])
            };
            status_ok(execute(Command::ControlStateChangeAdd {
                key,
                timestamp_ms,
                value: args[3].clone(),
                precision_ms,
                ttl_ms,
            }))
        }
        "CONTROLSTATECOUNT" if args.len() == 4 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            integer_response(execute(Command::ControlStateCount {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
            }))
        }
        "CONTROLSTATEQUERY" if args.len() == 5 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            integer_response(execute(Command::ControlStateQuery {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                aggregator: string_arg(&args[4]),
            }))
        }
        "CONTROLSTATEDETAIL" if args.len() == 4 || args.len() == 5 => {
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
            feature_points_response(execute(Command::ControlStateDetail {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
                count,
            }))
        }
        "CONTROLSTATEHSET" | "CPCSET" | "FOLSET" if args.len() == 4 => {
            let timestamp_ms = match parse_u64(&args[2], "timestamp_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let amount = match parse_i64_arg(&args[3], "amount") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            status_ok(execute(Command::ControlStateSet {
                family: control_state_family_for_command(&command),
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
                "FIRST" => ControlStateFolType::First,
                "LAST" => ControlStateFolType::Last,
                value => return RespValue::Error(format!("ERR unsupported fol_type: {value}")),
            };
            status_ok(execute(Command::ControlStateFolSet {
                key: string_arg(&args[1]),
                value: args[2].clone(),
                occur_time_ms,
                ttl_ms,
                fol_type,
            }))
        }
        "FOLQUERY" if args.len() == 2 => bytes_response(execute(Command::ControlStateFolQuery {
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
            integer_response(execute(Command::ControlStateFamilyQuery {
                family: control_state_family_for_command(&command),
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
            integer_response(execute(Command::ControlStateSetAndGet {
                family: control_state_family_for_command(&command),
                key: string_arg(&args[1]),
                timestamp_ms,
                amount,
                start_ms,
                end_ms,
                aggregator: string_arg(&args[6]),
            }))
        }
        "HSETANDGETOPT" | "CPCSETANDGETOPT" | "FOLSETANDGETOPT" if args.len() == 10 => {
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
            let precision_ms = match parse_u64(&args[7], "precision_ms") {
                Ok(0) => None,
                Ok(value) => Some(value),
                Err(err) => return RespValue::Error(err),
            };
            let ttl_ms = match parse_u64(&args[8], "ttl_ms") {
                Ok(0) => None,
                Ok(value) => Some(value),
                Err(err) => return RespValue::Error(err),
            };
            let uuid = {
                let raw = string_arg(&args[9]);
                if raw.is_empty() {
                    None
                } else {
                    Some(raw)
                }
            };
            integer_response(execute(Command::ControlStateSetAndGetWithOptions {
                family: control_state_family_for_command(&command),
                key: string_arg(&args[1]),
                timestamp_ms,
                amount,
                start_ms,
                end_ms,
                aggregator: string_arg(&args[6]),
                precision_ms,
                ttl_ms,
                uuid,
            }))
        }
        "CONTROLSTATEMANAGER" if args.len() == 2 => hash_entries_response(execute(Command::ControlStateManager {
            key: string_arg(&args[1]),
            op_type: None,
            field_list: Vec::new(),
            start_offset: String::new(),
            end_offset: String::new(),
            is_cpc: false,
        })),
        // C++ MANAGER op parity: CONTROLSTATEMANAGER key op_type is_cpc [start_offset end_offset | field...]
        // op_type: QUERY(2)|FIELD_LIST(5)|FIELD_COUNT(6)|ALL_DATA_VALUE(7). is_cpc: 0/1.
        "CONTROLSTATEMANAGER" if args.len() >= 4 => {
            let op_type = string_arg(&args[2]);
            let is_cpc = matches!(string_arg(&args[3]).as_str(), "1" | "cpc" | "true");
            let op = op_type.trim().to_ascii_uppercase();
            let is_field_list = op == "5" || op == "FIELD_LIST";
            let is_query = op == "2" || op == "QUERY";
            let (field_list, start_offset, end_offset) = if is_field_list && args.len() >= 6 {
                (Vec::new(), string_arg(&args[4]), string_arg(&args[5]))
            } else if is_query {
                (
                    args[4..]
                        .iter()
                        .map(|arg| (string_arg(arg), String::new()))
                        .collect(),
                    String::new(),
                    String::new(),
                )
            } else {
                (Vec::new(), String::new(), String::new())
            };
            hash_entries_response(execute(Command::ControlStateManager {
                key: string_arg(&args[1]),
                op_type: Some(op_type),
                field_list,
                start_offset,
                end_offset,
                is_cpc,
            }))
        }
        "CONTROLSTATEDEBUG" if args.len() == 4 => {
            let start_ms = match parse_u64(&args[2], "start_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            let end_ms = match parse_u64(&args[3], "end_ms") {
                Ok(value) => value,
                Err(err) => return RespValue::Error(err),
            };
            hash_entries_response(execute(Command::ControlStateDebug {
                key: string_arg(&args[1]),
                start_ms,
                end_ms,
            }))
        }
        _ => RespValue::Error(format!("ERR unsupported command or arity: {command}")),
    }
}
