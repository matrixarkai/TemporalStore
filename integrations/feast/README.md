<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->

# TemporalStore as a Feast online store

[Feast](https://github.com/feast-dev/feast) resolves its online store through a plugin class
named in `feature_store.yaml`. This folder is that plugin, serving feature values out of
TemporalStore over its RESP surface.

```yaml
project: demo
registry: data/registry.db
provider: local
online_store:
    type: feast_temporalstore.online_store.TemporalStoreOnlineStore
    connection_string: localhost:6399
    key_ttl_seconds: 86400
```

Feast requires the class name to end in `OnlineStore` and imports `<ClassName>Config` from the
same module, so `TemporalStoreOnlineStore` and `TemporalStoreOnlineStoreConfig` are both
load-bearing names, not style.

## What Feast asks of an online store, call by call

Taken from Feast's own Redis online store rather than from its documentation, because the
documentation does not say which commands are issued. Every command Feast needs is one we
already dispatch — **the command coverage is complete, with nothing missing.**

| Feast operation | Commands it issues | We dispatch it | Where |
|---|---|---|---|
| `online_write_batch`, dedup read | `HMGET key _ts:<view>` | yes | `redis_dispatch.rs:619` |
| `online_write_batch`, the write | `HSET key field value …` (multi-pair) | yes | `redis_dispatch.rs:532` |
| `online_write_batch`, TTL | `EXPIRE key seconds` | yes | `redis_dispatch.rs:408` |
| `online_read` | `HMGET key field …` | yes | `redis_dispatch.rs:619` |
| `update` / `teardown`, enumerate | `SCAN cursor MATCH pat COUNT n` | yes | `redis_dispatch.rs:1256`, `keyspace_commands.rs:81` |
| `update`, inspect a row | `HKEYS key` | yes | `redis_dispatch.rs:656` |
| `update` / `teardown`, remove | `DEL key`, `HDEL key field …` | yes | `redis_dispatch.rs:330`, `:713` |
| all of the above | request pipelining | yes | `redis_protocol.rs`, `read_until` + `read_exact` over a `BufRead` |

Feast never issues `HGETALL` or `UNLINK` on this path, and its optional GLIDE client is a second
client library rather than a second command set, so the redis-py path above is the whole surface.

## Two server behaviours that make the obvious configuration silently wrong

Both are why this plugin exists at all instead of a one-line `type: redis` pointed at our port.
Both produce wrong answers rather than errors, which is why they are worth stating precisely.

### 1. Keys and hash fields are decoded lossily; values are not

`string_arg` is `String::from_utf8_lossy`, and the dispatch passes the key and the hash field
through it on every command in the table above. Values survive — they stay `Vec<u8>` on the way
in and come back as `RespValue::Bulk(Option<Vec<u8>>)`. The wire layer is not at fault:
`read_command` reads bulk strings by length into `Vec<u8>`, so the bytes arrive intact and are
discarded one layer up.

Feast addresses a row by bytes that are not text:

- `_redis_key(project, entity_key)` returns `serialize_entity_key(entity_key) + project.encode("utf-8")` — a packed binary structure, so arbitrary bytes.
- `_mmh3(f"{view}:{feature}")` returns **exactly four arbitrary bytes**, the little-endian murmur3_32 of the name.

Measured, not assumed:

| quantity | value | how |
|---|---|---|
| 4-byte strings that are valid UTF-8 | **8.9237%** | exact, by dynamic programming over the byte alphabet, agreeing with an exhaustive check of all 2^24 three-byte strings |
| 4-byte field names that do **not** survive | **91.0763%** | the complement of the above |
| two distinct field names collide | **1 in 1,042** | 4,000,000 samples through maximal-subpart replacement |
| a correct store would collide | 1 in 4,294,967,296 | the hash is 32 bits |
| field names collapsing onto one single string | **2.89%** of all possible names | the class `'����'` |

So the collision rate rises by a factor of about four million. Per feature view:

| features in a view | expected colliding pairs | chance of at least one |
|---|---|---|
| 10 | 0.04 | 4.2% |
| 25 | 0.29 | 25.0% |
| 50 | 1.18 | **69.1%** |
| 100 | 4.75 | **99.1%** |
| 200 | 19.1 | ~100% |

A collision means two features share a hash field: one overwrites the other, and `online_read`
returns the wrong feature's value with no error anywhere. `HKEYS` makes it unrecoverable, because
it hands back the already-mangled field rather than the bytes that were written.

**What this plugin does about it, temporarily.** `feast_temporalstore/wire.py` encodes both halves
into byte ranges the conversion leaves alone — hex for keys, so `SCAN MATCH` prefixes and suffixes
still work, and unpadded base64url for fields, which are never pattern-matched. The cost is real:
a stored key doubles in length and a field grows by a third.

**That encoding is a workaround and is meant to be deleted — it is not the intended wire format.**
But fixing the server is not a one-line change, which is why the workaround is the right call for
now. `string_arg` is `String::from_utf8_lossy(value).to_string()` at
`redis/response_helpers.rs:115`, reached from 149 call sites — and the conversion is not confined
to the dispatch. The engine's command type itself declares those positions as text:

```rust
Command::HashSet { key: String, field: String, value: Vec<u8> }
```

with the bytes codec (`#[serde(with = "crate::bytes_serde")]`) on **`value` only**. That type is
what `wal_record.rs` serialises into the write-ahead log and replicates, so making a key or field
binary changes the **durable command encoding** — a format-versioned, reader-first change, not a
patch to the RESP layer.

**The deletion condition, both halves required.** This encoding can be dropped only once the
server guarantees **both** that `HKEYS`/`HGETALL` round-trip arbitrary byte names back identically
**and** that two distinct byte names never collide. A fix that stops the collision while still
returning a mangled name does not free it: `_drop_view` reads field names back and must match them
against the ones it wrote. Until both hold the encoding is load-bearing, and
`tests/test_wire.py::test_raw_field_names_collide_under_the_server_and_encoded_ones_do_not` fails
with instructions if anyone removes it early.

### 2. `SCAN` cannot see another connection's keys

`RedisCommandState` is constructed per accepted socket in `redis_proxy_server.rs`, and its
`keyspace: HashSet<String>` is the set `SCAN`, `KEYS`, `DBSIZE` and `RANDOMKEY` enumerate. A key
written on one connection is invisible to a scan on another, and invisible to every later process.

Feast's cleanup is exactly that shape: `update()` and `teardown()` call `scan_iter(...)` from a
fresh client, usually in a separate `feast apply` or `feast teardown` invocation. The scan returns
an empty cursor, Feast concludes there is nothing to remove, and reports success. Materialized
rows are left behind indefinitely.

**What this plugin does about it.** It records each view's row keys in a set as it writes them
(`ts:feast:rows:<project>:<view>`) and enumerates that set to clean up, so no scan is needed.

**The registry is a deliberate improvement, not only a workaround — do not "simplify" it back to
`SCAN`.** It would be the better design even on a store whose scan saw the whole keyspace, for two
reasons that have nothing to do with the defect above. It is `O(rows this view wrote)` where a
scan is `O(the entire keyspace)`, and it names its rows exactly rather than matching a glob that
can sweep in another project's keys. Feast's own Redis store concedes the point by hiding its scan
behind a `full_scan_for_deletion` switch, because the scan is slow enough that users turn it off —
and turning it off there means cleanup silently stops happening. The registry has no such
trade-off. So if the per-connection `SCAN` behaviour is ever fixed, that is a reason to delete the
paragraph above, not this code.

## Layout

```
integrations/feast/
    README.md                            this file
    pyproject.toml                       installable as feast-temporalstore
    feast_temporalstore/
        __init__.py
        wire.py                          the byte-safe encoding, and why
        online_store.py                  the OnlineStore implementation
    tests/
        test_wire.py                     6 tests, no server and no Feast needed
    benchmarks/
        bench_online_store.py            the compared benchmark, one arm per invocation
        feature_store.temporalstore.yaml
        feature_store.redis.yaml
```

## Performance, and what is not yet measured

The benchmark drives both arms through Feast itself, so the only difference between them is the
`online_store` block. It reports `get_online_features` p50/p99, materialization throughput, and
bytes on disk per feature row with the denominator named in the output.

**No numbers are published here yet, and that is deliberate.** The machine this was developed on
was running a load average between 89 and 109 on 16 cores throughout, with 8.3 GB of disk free —
too little to install Feast's dependency tree and a Redis alongside it. A p99 taken under that
load is a measurement of the load. The benchmark therefore reads the load average before and after
every phase, prints it beside every figure, and **exits non-zero with the latency labelled
`PROVISIONAL` when the peak exceeds `--max-load`**, so a loaded run cannot be mistaken for a quiet
one. Run both arms on an idle box and the table fills itself in.

What is verified today: the command coverage table above, both server findings, and the encoding,
which carries 6 tests that need neither Feast nor a running store. What is not: an end-to-end run
against a live Feast and a live TemporalStore, and therefore the method signatures against the
installed `OnlineStore` abstract base class.
