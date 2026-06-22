# MatrixArk Entity Update Algorithm

## Goal

Avoid a second LLM call when the user only needs updated entity memory.

The one-pass extractor emits both:

- new events;
- entity updates;
- compact field patches.

MatrixArk then applies those patches deterministically with approximate span
matching. This is inspired by VikingMem's faster entity update path: the model
does one extraction pass, and entity maintenance is a deterministic
post-processing step.

## Patch Format

```text
<< SEARCH
old field span
====
new field content
>> REPLACE
```

Example:

```text
<< SEARCH
coffee
====
jasmine tea
>> REPLACE
```

## Algorithm

```text
one-pass extraction emits EntityPatch
-> locate previous entity by node_hash + entity_type + entity_name
-> parse SEARCH/REPLACE patch
-> exact search first
-> approximate edit-distance span search if exact search misses
-> replace best matching span
-> write new ContextEntity version
-> write ContextEntityUpdateAudit with llm_calls=0
```

This does not enumerate all stored entities. The current implementation uses the
entity hash derived from:

```text
node_hash + entity_type + entity_name
```

## Why It Matters

Without EUA:

```text
batch messages -> LLM extracts event
old entity -> second LLM call to merge/update entity
```

With EUA:

```text
batch messages -> one LLM extraction emits patch
old entity + patch -> deterministic update
```

Benefits:

- lower latency;
- lower token cost;
- replayable update audit;
- robust to minor mismatch through approximate search;
- no additional LLM pass for entity maintenance.

## Records

`ContextEntity` stores:

- `state`
- `previous_state`
- `field_patches`
- `patch_results`
- `update_mode`

`ContextEntityUpdateAudit` stores:

- `previous_state`
- `new_state`
- `patch_results`
- `llm_calls = 0`
- `update_mode = deterministic_eua`

## Validation

Local:

```bash
python3 tools/run_matrixark_entity_update_algorithm_test.py \
  --backend local \
  --report-json /tmp/matrixark_eua_local.json
```

C++ TemporalStore direct:

```bash
PYTHONPATH=$PWD/sdk/python \
TEMPORALSTORE_LIB=$PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
python3 tools/run_matrixark_entity_update_algorithm_test.py \
  --backend temporalstore-direct \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --temporalstore-lib $PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --report-json /tmp/matrixark_eua_cpp_direct.json
```
