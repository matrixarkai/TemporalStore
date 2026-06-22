# ABase Redis Protocol Test - 2026-06-06

## Goal

Validate whether the ABase process currently deployed on the shared AWS cluster can be exercised through Redis RESP commands and later benchmarked with `redis-cli` / `redis-benchmark`.

## Cluster

- Meta/proxy node: `i-003c930417f7ee609`, private IP `10.70.1.79`
- Data node 1: `i-0724d90b323786546`, private IP `10.70.1.163`
- Data node 2: `i-096334bd8cc7ab259`, private IP `10.70.1.202`

## Service Status

The ABase services were alive:

- `abase-master` running on the meta node.
- `abase-proxy` running on the meta node.
- `abase-datanode` running on both data nodes.
- Master `ListDatanode` reported both datanodes in normal state.

Observed proxy ports:

```text
19074 master
19077 proxy listener
19078 proxy listener listed by master
```

Master `ListProxy` reported:

```text
proxy_addr: 10.70.1.79:19078
namespace: aws_scale
table: bench
protocol_type: ABASE2_THRIFT_PROTOCOL
```

That last field matters: the currently registered proxy is not advertised as `REDIS_PROTOCOL`.

## Test Runs

### First Redis CLI Probe

Initial script:

```text
tools/aws_abase_redis_api_benchmark.sh
```

The SSM execution wrapper was fixed to avoid `/bin/sh` rejecting `set -o pipefail`. The script now writes the benchmark body to `/tmp/aws_abase_redis_api_benchmark.sh` on the target node and runs it with bash.

Result:

The Redis tools were installed successfully on the meta node, but the first Redis command did not complete:

```text
redis-cli -h 127.0.0.1 -p 19078 --raw PING
```

Result:

```text
timeout after 30 seconds
```

### Raw RESP And Redis CLI Probe

SSM command:

```text
882029ec-99b9-49a7-88d0-045878d2da3d
```

Remote result directory:

```text
/var/lib/abase/abase_redis_protocol_20260606T075927Z
```

The probe tested both raw TCP RESP frames and `redis-cli` against ports `19077` and `19078` for:

```text
PING
ECHO hello
SET abase:redis:proto:string v1
GET abase:redis:proto:string
INCR abase:redis:proto:counter
HSET abase:redis:proto:hash field1 value1
HGET abase:redis:proto:hash field1
MGET abase:redis:proto:string abase:redis:proto:missing
DEL abase:redis:proto:string
```

Observed behavior:

| Port | Probe | Result |
|---:|---|---|
| `19077` | raw RESP `PING`, `ECHO` | timed out around 3 seconds |
| `19077` | raw RESP writes/reads | connection closed or empty response, not a valid Redis reply |
| `19077` | `redis-cli` | `PING` / `ECHO` timed out; other commands returned `Error: Server closed the connection` |
| `19078` | raw RESP `ECHO hello` | returned `-ERR unknown command \`echo\`` |
| `19078` | `redis-cli ECHO hello` | returned the same unknown-command error |
| `19078` | raw RESP / `redis-cli` for `PING`, `SET`, `GET`, `INCR`, `HSET`, `HGET`, `MGET`, `DEL` | timed out |

Interpretation:

- Port `19078` parses at least some RESP framing because it returned a Redis-style error for `ECHO`.
- The common Redis commands we need for scale testing did not work on this deployment.
- Port `19077` is not usable as a Redis RESP data endpoint in the current configuration.
- The proxy is registered as `ABASE2_THRIFT_PROTOCOL`, so the result is consistent with a proxy started in ABase2/Thrift mode rather than Redis compatibility mode.

## Source-Code Clues

Local ABase source shows two related but different paths:

- `src/proxy/metrics.cc` maps protocol names including `abase2_thrift` and `redis`.
- `src/proxy/redis_protocol.cc` contains Redis protocol handling code.
- `local_sdks/python/abase_proxy_client.py` is an ABase2 Thrift proxy client and supports `set`, `get`, `set_many`, and `get_many`.

The code therefore appears to contain a Redis protocol path, but the AWS process tested here is not currently registered or behaving as that path.

## Current Conclusion

ABase direct-path testing works, but ABase Redis protocol testing is not yet usable on the current AWS deployment.

ABase Redis scale testing is blocked until one of these is done:

- start/register the proxy with `protocol_type=REDIS_PROTOCOL`,
- confirm the exact Redis-compatible port if it is different from `19077` / `19078`,
- confirm whether Redis keys need table routing syntax such as `[table]real_key`,
- then rerun `redis-cli` command coverage before using `redis-benchmark`.

This is not a datanode-registration failure. The master saw both datanodes as normal.

## Next Test Plan

1. Find the proxy flag or config path that switches the listener from `ABASE2_THRIFT_PROTOCOL` to `REDIS_PROTOCOL`.
2. Restart only the ABase proxy on the shared meta node, keeping the same master and datanodes.
3. Run command coverage first: `PING`, `SET`, `GET`, `DEL`, `INCR`, `MGET`, `HSET`, `HGET`.
4. If coverage passes, run scale tests with `redis-benchmark` and a small custom script for hash commands.
5. Keep ABase2 Thrift proxy testing separate from Redis protocol testing so we do not mix protocol results.
