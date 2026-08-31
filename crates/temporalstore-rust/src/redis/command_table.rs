// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis supported-command descriptor table, extracted from redis.rs.

use super::*;

pub(crate) fn redis_supported_commands() -> &'static [RedisCommandDescriptor] {
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
            name: "BUCKETPEEK",
            arity: 5,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "BUCKETTAKE",
            arity: 5,
            flags: WRITE,
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
            name: "INCRBYFLOAT",
            arity: 3,
            flags: WRITE,
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
            name: "LPUSH",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LRANGE",
            arity: 4,
            flags: READ,
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
            name: "PERSIST",
            arity: 2,
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
            name: "RPUSH",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "SEENCARD",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "SEENCHECK",
            arity: 4,
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
            name: "ZCOUNT",
            arity: 4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZINCRBY",
            arity: 4,
            flags: WRITE,
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
            name: "ZRANK",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZREVRANK",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZCARD",
            arity: 2,
            flags: READ,
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
            name: "ZREVRANGE",
            arity: -4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZREVRANGEBYSCORE",
            arity: -4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "ZSCORE",
            arity: 3,
            flags: READ,
        },    ]
}
