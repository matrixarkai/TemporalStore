use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::types::{Command, CommandResponse, ExecuteRequest, FeaturePoint, ShardId};
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
    while let Some(args) = read_command(&mut reader)? {
        let response = execute_redis_command(args, shard_id, |command| {
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

pub fn execute_redis_command(
    args: Vec<Vec<u8>>,
    _shard_id: ShardId,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR empty command".to_string());
    }
    let command = upper(&args[0]);
    match command.as_str() {
        "PING" => RespValue::SimpleString(
            args.get(1)
                .map(|value| String::from_utf8_lossy(value).to_string())
                .unwrap_or_else(|| "PONG".to_string()),
        ),
        "GET" if args.len() == 2 => bytes_response(execute(Command::StringGet {
            key: string_arg(&args[1]),
        })),
        "SET" if args.len() == 3 || args.len() == 5 => {
            let key = string_arg(&args[1]);
            let value = args[2].clone();
            let command = match parse_set_ttl_ms(&args[3..]) {
                Ok(Some(ttl_ms)) => Command::StringSetEx { key, value, ttl_ms },
                Ok(None) => Command::StringSet { key, value },
                Err(err) => return RespValue::Error(err),
            };
            status_ok(execute(command))
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
        "DEL" if args.len() >= 2 => {
            let mut removed = 0;
            for key in args.iter().skip(1) {
                if execute(Command::CommonDelete {
                    key: string_arg(key),
                })
                .is_ok()
                {
                    removed += 1;
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
        "HSET" if args.len() == 4 => integer_ok(execute(Command::HashSet {
            key: string_arg(&args[1]),
            field: string_arg(&args[2]),
            value: args[3].clone(),
        })),
        "HMSET" if args.len() >= 4 && args.len() % 2 == 0 => {
            for pair in args[2..].chunks(2) {
                if let Err(err) = execute(Command::HashSet {
                    key: string_arg(&args[1]),
                    field: string_arg(&pair[0]),
                    value: pair[1].clone(),
                }) {
                    return RespValue::Error(format!("ERR {err}"));
                }
            }
            RespValue::SimpleString("OK".to_string())
        }
        "HGET" if args.len() == 3 => bytes_response(execute(Command::HashGet {
            key: string_arg(&args[1]),
            field: string_arg(&args[2]),
        })),
        "HMGET" if args.len() >= 3 => {
            let mut values = Vec::new();
            for field in args.iter().skip(2) {
                match execute(Command::HashGet {
                    key: string_arg(&args[1]),
                    field: string_arg(field),
                }) {
                    Ok(CommandResponse::Bytes { value }) => values.push(RespValue::Bulk(value)),
                    Ok(_) => return RespValue::Error("ERR invalid hmget response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Array(values)
        }
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
        "HDEL" if args.len() == 3 => integer_ok(execute(Command::HashDelete {
            key: string_arg(&args[1]),
            field: string_arg(&args[2]),
        })),
        "SADD" if args.len() == 3 => integer_ok(execute(Command::SetAdd {
            key: string_arg(&args[1]),
            member: args[2].clone(),
        })),
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
        "SREM" if args.len() == 3 => integer_ok(execute(Command::SetRemove {
            key: string_arg(&args[1]),
            member: args[2].clone(),
        })),
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
        _ => RespValue::Error(format!("ERR unsupported command or arity: {command}")),
    }
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

fn parse_set_ttl_ms(args: &[Vec<u8>]) -> Result<Option<u64>, String> {
    if args.is_empty() {
        return Ok(None);
    }
    if args.len() != 2 {
        return Err("ERR syntax error".to_string());
    }
    match upper(&args[0]).as_str() {
        "EX" => parse_u64(&args[1], "seconds").map(|seconds| Some(seconds.saturating_mul(1000))),
        "PX" => parse_u64(&args[1], "milliseconds").map(Some),
        _ => Err("ERR syntax error".to_string()),
    }
}

fn expire_response(
    args: &[Vec<u8>],
    factor: u64,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    match parse_u64(&args[2], "ttl") {
        Ok(ttl) => integer_ok(execute(Command::CommonExpire {
            key: string_arg(&args[1]),
            ttl_ms: ttl.saturating_mul(factor),
        })),
        Err(err) => RespValue::Error(err),
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

fn integer_ok(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(_) => RespValue::Integer(1),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
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
        assert_eq!(run(vec!["GET", "k"]), RespValue::Bulk(Some(b"v".to_vec())));
        assert_eq!(run(vec!["HSET", "h", "f", "x"]), RespValue::Integer(1));
        assert_eq!(
            run(vec!["HGET", "h", "f"]),
            RespValue::Bulk(Some(b"x".to_vec()))
        );
        assert_eq!(run(vec!["SADD", "s", "m"]), RespValue::Integer(1));
        assert_eq!(
            run(vec!["SMEMBERS", "s"]),
            RespValue::Array(vec![RespValue::Bulk(Some(b"m".to_vec()))])
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
    }
}
