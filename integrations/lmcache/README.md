# LMCache L2 storage on TemporalStore

This package provides an LMCache multiprocess L2 adapter. KV payloads use the
datanode's raw `PUT` and `GET /blob/<key>` route. Existence, expiry, and deletion
use the datanode command protocol through the existing Feast TemporalStore
client. The adapter also uses `DELETE /blob/<key>` to reclaim payloads when it
evicts entries. Explicit batch deletion uses the datanode's
`POST /batch_delete_blobs` route when available and falls back to individual
DELETE requests on older datanodes.

Install the Feast integration and this package from the same TemporalStore
checkout, alongside an LMCache installation with its native module available:

```bash
python -m pip install ./integrations/feast ./integrations/lmcache
```

Point both URLs at a datanode that serves the selected shard. The blob and
command routes must address the same node:

```json
{
  "type": "plugin",
  "module_path": "lmcache_temporalstore.adapter",
  "class_name": "TemporalStoreL2Adapter",
  "adapter_params": {
    "blob_url": "http://127.0.0.1:17002",
    "data_url": "http://127.0.0.1:17002",
    "shard_id": 1,
    "max_capacity_gb": 64,
    "ttl_seconds": 0
  }
}
```

The capacity budget is maintained by each adapter process. Configure one
adapter writer per blob namespace when relying on this budget. Metadata expiry
does not itself remove blob files; use adapter deletion or eviction to reclaim
their payloads. `ttl_seconds=0` keeps metadata until it is explicitly removed.


## Distributed vLLM deployment

Run an LMCache multiprocess server beside each vLLM worker host. The vLLM
connector exchanges KV tensors with its local LMCache server through shared
memory. The LMCache server can access a TemporalStore datanode over the
network. Keep both adapter URLs pointed at the datanode serving the
configured shard.

Give each LMCache server a distinct, stable `namespace` when several servers
use the same datanode. One LMCache server owns the writes, locks, and eviction
budget for its namespace. Workers attached to that server can reuse its
cache. The default empty namespace preserves keys written by older versions.
Reuse the namespace on restart to find its previous cache entries. Capacity
accounting covers writes made since the adapter started. Each namespace
has its own cache budget. Route a namespace to the same datanode to keep
the metadata and blob operations together.

Save the adapter JSON above in `temporalstore-l2.json`, set its URLs and
`namespace`, set `remote_storage` to `true` for a datanode on another
host, and run:

```bash
lmcache server --port 5555 --l1-size-gb 2 --eviction-policy LRU \
  --l2-adapter "$(cat temporalstore-l2.json)"
```

On the same host, connect vLLM using the LMCache multiprocess connector:

```bash
vllm serve "$MODEL" --no-enable-prefix-caching --kv-transfer-config \
  '{"kv_connector":"LMCacheMPConnector","kv_connector_module_path":"lmcache.integration.vllm.lmcache_mp_connector","kv_role":"kv_both","kv_connector_extra_config":{"lmcache.mp.port":5555}}'
```

For CPU vLLM, also set `lmcache.mp.mp_transfer_mode` to `engine_driven`
in the connector extra config, pass `--disable-hybrid-kv-cache-manager` to
vLLM, and run the LMCache server with
`--supported-transfer-mode engine_driven`.

To verify L2 reuse, send a prompt longer than one LMCache chunk, wait for the
L2 adapter to report a store in LMCache's `/status`, clear LMCache L1 with
`POST /cache/clear` on the test server, then send the same prompt. The L2
adapter's `loads` counter must increase. Leaving L1 warm can produce a hit
without reading from TemporalStore. To test handoff between two vLLM
processes attached to this LMCache server, pass the second process's URL
with `--reload-vllm-url`. Use the same model and KV layout on both processes.

Run the check against dedicated test servers with prefix caching disabled:

```bash
python integrations/lmcache/tests/vllm_smoke.py --model "$MODEL"
```


### Replicas across datanodes

For L2 redundancy, configure one TemporalStore adapter per independent
datanode. Each adapter's blob and command URLs must point to that same
datanode. Use separate backing storage for the datanodes. LMCache's
`default` store policy sends each chunk to every configured adapter and
prefetch can use any adapter that has the chunk.

```bash
lmcache server --port 5555 --l1-size-gb 2 --eviction-policy LRU \
  --l2-store-policy default \
  --l2-adapter "$(cat temporalstore-primary.json)" \
  --l2-adapter "$(cat temporalstore-replica.json)"
```

Run the smoke test with both datanodes available. Its output lists the store
and load deltas for each adapter in configuration order. On a dedicated test
deployment, verify failover by warming a fresh prompt, waiting for both
adapters to record stores, stopping one datanode, clearing L1, and repeating
the prompt. The available adapter's load counter should increase. Each
adapter applies its own capacity budget. A chunk is available from a
replica after the replica has accepted its write.

A metadata lookup waits up to one second per request by default when
`remote_storage` is `true` and `timeout_seconds` is one second or longer.
Set `lookup_timeout_seconds` to tune this wait for your network and
datanodes. After a transport failure, that adapter reports metadata misses for
one second before retrying on the next lookup. This lets reachable replicas
serve queued requests while the link recovers. Blob transfers and metadata
writes still use `timeout_seconds`.

To measure the L2 path independent of model inference, use LMCache's adapter
benchmark against a disposable datanode:

```bash
lmcache bench l2 --l2-adapter "$(cat temporalstore-l2.json)" \
  --num-keys 16 --data-size-kb 1024 --warmup-rounds 2 --rounds 10 \
  --lookup-max-hit-rate 1
```
