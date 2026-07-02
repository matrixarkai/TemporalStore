# MatrixArk C++ TemporalStore Direct Data Model Query Runbook

This run stores the MatrixArk ingestion, extraction, retrieval, L0/L1 summary, segment, entity, index, embedding, and ContextPack audit records in real C++ TemporalStore through the native Python SDK binding.

## Current C++ Run

```text
backend: temporalstore-direct
metaserver: 127.0.0.1:18000
namespace: deploy_ns
table: deploy_table
storage_prefix: matrixark:cpp:query:index:debug:1782231394422
temporalstore_lib: <repo>/output-ubuntu22/release/sdk/lib/libbcache2.so
embedding_model: <repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2
embedding_dim: 384
```

C++ readback verified:

```text
record_count: 230
first_record: ContextEvent payload read from hget(prefix + ':records:000000', '00000000000000000000')
```

## Re-run The Full C++ Flow

```bash
cd <repo>
MATRIXARK_EMBEDDING_PROVIDER=oss MATRIXARK_REQUIRE_OSS_EMBEDDINGS=1 MATRIXARK_EMBEDDING_MODEL_PATH=<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2 PYTHONPATH=. TEMPORALSTORE_LIB=<repo>/output-ubuntu22/release/sdk/lib/libbcache2.so python3 tools/run_matrixark_query_index_debug_flow.py   --backend temporalstore-direct   --metaserver 127.0.0.1:18000   --namespace deploy_ns   --table deploy_table   --temporalstore-lib <repo>/output-ubuntu22/release/sdk/lib/libbcache2.so   --storage-prefix matrixark:cpp:query:index:debug:$(date +%s%3N)   --artifact-dir .local/context-debug/query-index-cpp-direct-debug
```

## How Records Are Stored In C++ TemporalStore

MatrixArk direct mode stores an append-only logical record log:

```text
{storage_prefix}:record_count       string count
{storage_prefix}:records:000000     hash shard 0
{storage_prefix}:records:000001     hash shard 1 if needed
...
```

Each hash field is a zero-padded sequence id. The field value is one JSON MatrixArk record.

## Query All Records By Data Model

```bash
cd <repo>
PYTHONPATH=sdk/python python3 - <<'PY'
import json
from collections import Counter
from temporalstore import Client, Options

prefix = 'matrixark:cpp:query:index:debug:1782231394422'
opts = Options(
    metaserver_addr='127.0.0.1:18000',
    namespace_name='deploy_ns',
    table_name='deploy_table',
    request_timeout_ms=60000,
    io_timeout_ms=60000,
)
client = Client(opts, library_path='output-ubuntu22/release/sdk/lib/libbcache2.so')
count = int(client.get_string(prefix + ':record_count'))
records = []
for i in range(count):
    shard = i // 256
    field = f'{i:020d}'
    key = f'{prefix}:records:{shard:06d}'
    raw = client.hget(key, field)
    if raw:
        records.append(json.loads(raw))
client.close()

print('record_count', count)
print(Counter(r.get('record_type') for r in records))
for model in ['context_event', 'context_segment', 'context_entity', 'context_summary', 'context_embedding', 'context_index', 'context_pack_audit']:
    rows = [r for r in records if r.get('record_type') == model]
    print('
==', model, len(rows), '==')
    print(json.dumps(rows[:2], indent=2)[:4000])
PY
```

## Query Only Secondary Indexes

```bash
PYTHONPATH=sdk/python python3 - <<'PY'
import json
from temporalstore import Client, Options
prefix = 'matrixark:cpp:query:index:debug:1782231394422'
opts = Options(metaserver_addr='127.0.0.1:18000', namespace_name='deploy_ns', table_name='deploy_table')
client = Client(opts, library_path='output-ubuntu22/release/sdk/lib/libbcache2.so')
records=[]
for i in range(int(client.get_string(prefix + ':record_count'))):
    raw = client.hget(f'{prefix}:records:{i//256:06d}', f'{i:020d}')
    if raw:
        r=json.loads(raw)
        if r.get('record_type') == 'context_index':
            records.append(r)
client.close()
for r in records:
    print(r['index_name'], 'batch=', r.get('batch_id_hash'), 'node=', r.get('node_path'))
PY
```

## Query Embedding Metadata

```bash
PYTHONPATH=sdk/python python3 - <<'PY'
import json
from temporalstore import Client, Options
prefix='matrixark:cpp:query:index:debug:1782231394422'
opts=Options(metaserver_addr='127.0.0.1:18000', namespace_name='deploy_ns', table_name='deploy_table')
client=Client(opts, library_path='output-ubuntu22/release/sdk/lib/libbcache2.so')
for i in range(int(client.get_string(prefix + ':record_count'))):
    raw=client.hget(f'{prefix}:records:{i//256:06d}', f'{i:020d}')
    if not raw:
        continue
    r=json.loads(raw)
    if r.get('record_type') == 'context_embedding':
        print(r.get('embedding_type'), r.get('ref_type'), r.get('dim'), len(r.get('vector', [])), r.get('model'), r.get('node_path'))
client.close()
PY
```

## C++ Report Files

- `docs/matrixark_query_index_cpp_direct_debug_flow.md`
- `docs/matrixark_query_index_cpp_direct_debug_flow.html`
- `docs/matrixark_query_index_cpp_direct_debug_flow.json`
