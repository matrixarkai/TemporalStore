# Where configuration lives

One page for the question "I want to change X — where do I put it, and what wins?"

TemporalStore is configured by **environment variables**. Everything else — the config file, the
operator portal, the tenant policy knobs — is a way of setting those variables or of describing
them. There is one precedence chain and it is short:

```
built-in code default   <   config file   <   explicit environment variable
```

An explicitly exported variable always wins. `tools/matrixark_load_config.py` seeds a variable from
the config file **only if that variable is not already set**, so nothing you export by hand is ever
overwritten.

## The three ways to set something

**1. Export the variable.** Highest precedence, no file needed, and what every reader in the engine
and the gateway actually consults.

```bash
export MATRIXARK_RETRIEVAL_MIN_SCORE=0.05
```

**2. Put it in `config/temporalstore.toml`.** One documented file, grouped in sections. Launch
through the loader so the file is applied:

```bash
scripts/with_config.sh python3 tools/matrixark_v1_gateway.py
eval "$(python3 tools/matrixark_load_config.py --print-exports)"   # or apply it yourself
python3 tools/matrixark_load_config.py --dry-run                   # show what WOULD be exported
python3 tools/matrixark_load_config.py --print-mapping             # SECTION.key -> ENV_VAR
```

The file is found at `--config PATH`, then `$MATRIXARK_CONFIG_FILE`, then
`<repo>/config/temporalstore.toml`.

**3. Use the operator portal.** For the subset of variables offered there, the portal writes the
value and says whether it takes effect immediately (`live`) or needs a restart (`restart`).

## What each place is authoritative for

| place | holds | authoritative for |
|---|---|---|
| the environment | the values every reader consults | **the value in force** |
| `config/temporalstore.toml` | 86 keys in 10 sections | what a deployment sets without exporting by hand |
| `tools/matrixark_load_config.py` (`ENV_MAP`) | 130 `SECTION.key -> VARIABLE` pairs | which file key seeds which variable, and the precedence above |
| `tools/matrixark_gateway_config.py` (`SETTINGS`) | 143 portal settings, 32 of them folded in from the tenant knobs | what the portal shows: label, group, kind, default, live-or-restart, help |
| `tools/matrixark_tenant_policy.py` (`KNOBS`) | 32 tenant knobs | per-tenant policy; `_knob_settings()` folds these into the registry above, so they are not a separate surface |
| `docs/ops/temporalstore-engine-flags.md` | every `TS_*` / `MATRIXARK_*` / `TEMPORALSTORE_*` the engine reads | the complete list, **generated from source** |

The engine flag inventory is generated, not written by hand:

```bash
python3 tools/build_engine_flag_inventory.py .
```

If you add or rename a variable in the Rust engine, regenerate it in the same change — a test
fails otherwise, and the document's own header notes that its staleness is silent.

## Not every variable is on the portal, and that is deliberate

Most variables are internal. The portal offers the ones an operator is expected to turn; the rest
are reachable through the config file or the environment. A variable missing from the portal is not
a bug by itself.

## One caveat worth knowing before you search

For 38 variables the config file and the portal use **different key names for the same variable**.
Most differ only in the section — the file groups by subsystem, the portal by panel — but twelve
differ in the leaf name as well:

| in `config/temporalstore.toml` | on the portal |
| --- | --- |
| `limits.rl_ingest_rps` | `limits.ingest_rps` |
| `limits.rl_ingest_burst` | `limits.ingest_burst` |
| `limits.rl_retrieve_rps` | `limits.retrieve_rps` |
| `limits.rl_retrieve_burst` | `limits.retrieve_burst` |
| `limits.rl_blob_streams` | `limits.blob_streams` |
| `limits.quota_max_batch` | `limits.max_batch` |
| `limits.quota_max_body_bytes` | `limits.max_body_bytes` |
| `limits.quota_max_blob_bytes` | `limits.max_blob_bytes` |
| `retrieval.retrieval_min_score` | `retrieval.min_score` |
| `retrieval.skill_discovery` | `skills.discovery` |
| `extraction.embedding_model` | `embedding.model` |
| `storage.index_dump_oplog_gap_bytes` | `storage_engine.index_dump_wal_gap_bytes` |

**The variable is the same in every row** — the two registries have never disagreed about which
variable a key means, and a test asserts that. Only the key differs, so if you set one and cannot
find it on the page, search for the variable name instead.

The last row is not a mistake: the engine still honours `TS_INDEX_DUMP_OPLOG_GAP_BYTES`, the
previous name of that variable, and the config file keeps the older word in its key for that
reason. The line in `config/temporalstore.toml` says so.

## Changing a config key

Config keys are a published interface. This repository is used and forked widely, so renaming a key
breaks every deployment whose file already uses it. Treat a rename as a breaking change, not a
tidy-up, and prefer leaving a documented older name in place — as the storage key above does.
