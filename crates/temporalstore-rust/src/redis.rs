use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

mod encoding;
mod protocol;
mod server;
mod state;
mod keyspace_commands;
mod string_commands;
mod list_set_commands;
mod response_helpers;
mod command_table;
mod dispatch;
mod zset_commands;
mod zset_storage;

pub use protocol::{read_command, RespValue};
pub use server::serve_redis_proxy;
pub use state::RedisCommandState;
pub use dispatch::execute_redis_command_with_state;

use crate::client::{bucket_id_for_key, stable_key_hash};
use crate::types::{
    parse_cpp_feature_filters, Command, CommandResponse, FeatureFilter, FeatureFilterOp,
    FeaturePoint, FeatureWritePolicy, RiskFamily, RiskFolType, ShardId, StringSetCondition,
};

use encoding::{REDIS_LIST_ENCODING_PREFIX, REDIS_ZSET_ENCODING_PREFIX};
use keyspace_commands::*;
pub(crate) use list_set_commands::*;
pub(crate) use response_helpers::*;
pub(crate) use command_table::*;
pub(crate) use dispatch::*;
pub(crate) use string_commands::*;
pub(crate) use zset_storage::*;
use zset_commands::*;

pub fn execute_redis_command(
    args: Vec<Vec<u8>>,
    shard_id: ShardId,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let mut state = RedisCommandState::default();
    execute_redis_command_with_state(args, shard_id, &mut state, |command| execute(command))
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
            redis_supported_commands()
                .iter()
                .map(|command| RespValue::Bulk(Some(command.name.as_bytes().to_vec())))
                .collect(),
        ),
        Some(subcommand) if subcommand == "COUNT" && args.len() == 2 => {
            RespValue::Integer(redis_supported_commands().len() as i64)
        }
        Some(subcommand) if subcommand == "INFO" && args.len() >= 3 => {
            let commands = redis_supported_commands();
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


mod tests {
    use std::io::BufReader;

    use super::*;
    use crate::engine::TemporalEngine;
    use crate::types::{ExecuteRequest, SequenceFeatureRow};

    #[test]
    fn resp_parser_reads_array_command() {
        let mut input = BufReader::new(&b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"[..]);
        assert_eq!(
            read_command(&mut input).unwrap(),
            Some(vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()])
        );
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
