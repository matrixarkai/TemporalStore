# LMCache L2 storage on TemporalStore

This package provides an LMCache multiprocess L2 adapter. KV payloads use the
datanode's raw `PUT` and `GET /blob/<key>` route. Existence, expiry, and deletion
use the datanode command protocol through the existing Feast TemporalStore
client. The adapter also uses `DELETE /blob/<key>` to reclaim payloads when it
evicts entries.

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
