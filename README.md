# TemporalStore

**A distributed, time-aware serving engine, written in Rust.**
One durable store for the read paths that sit in front of a model or a request: ranked context,
long sequences, aggregated features, frequency caps, and control state.
Apache-2.0 · self-hostable · no vector database · [temporalstore.ai](https://temporalstore.ai)

📊 **Benchmarks & docs — [temporalstore.ai/benchmarks.html](https://temporalstore.ai/benchmarks.html)**

TemporalStore is general-purpose serving infrastructure. Every workload below is a first-class
tenant of the same temporal index — not a feature bolted onto a memory product. Keys are
time-ordered, writes are append-structured, and reads are answered from one engine rather than from
a vector database beside a feature store beside a counter tier. It runs locally with one Docker
command and scales out to a replicated, shared-storage cluster.

---

## What it serves

| | |
|---|---|
| 🧠 **Context management** | Ingest → extract → retrieve a ranked, token-budgeted **ContextPack**. Cross-session, cross-device, cross-agent. **No vector database.** |
| 📜 **Long sequences** | Append-mostly series per key, read by instant or window. Sorted owned arrays rather than a per-key tree, so a one-point series costs one allocation. |
| 📊 **Aggregated feature serving** | Exact count/sum/min/max/avg over high-cardinality keys, computed **on read** over a rollup ladder — no pre-aggregation pipeline to operate. |
| 🎛️ **Frequency capping** | Per-key counters with windowed sums, so "at most N in the last H hours" is a single read. |
| ⚙️ **Control state** | Quotas, pacing, suppression, selection — single-key **O(1)** atomic updates at serving time. |
| 🔌 **Speaks RESP** | A Redis-compatible surface (strings, hashes, sets, sorted sets, lists, control verbs) — existing Redis clients connect today. |

Underneath all six: an append-structured page store, no GC pauses, and crash-safe reload from its
own persistence.

## Why one engine

A serving stack for any of these workloads usually means running several systems — a vector store, a
feature store, a Redis-style counter tier, a stream pipeline, and something bespoke holding the
sequences. They disagree about time, and each one is a separate operational surface.

TemporalStore collapses them into one time-aware engine with a single index, a single durability
story, and one place to reason about recovery. For teams that pay per token, the context path also
bounds what gets replayed on every turn, which is usually the dominant cost.

## Quick start (single node, Docker)

```bash
docker run --rm -p 6379:6379 -p 8080:8080 \
  -v temporalstore-data:/var/lib/temporalstore \
  ghcr.io/matrixarkai/temporalstore:latest
```

Then talk to it with any Redis client:

```bash
# write: key "hello" = bytes for "world"
redis-cli -p 6379 SET hello world

# read it back
redis-cli -p 6379 GET hello
```

The RESP surface, the HTTP routes, and the control verbs are documented under
[`docs/`](docs/). Deploy manuals: [Windows Docker](docs/windows_docker_install.md) ·
[Linux](docs/linux_deploy.md) · [macOS](docs/macos_deploy.md).

## Python tooling and agent integrations

**This repository is the Rust engine only.** The Python context tooling — ingestion, retrieval,
feedback, the agent hooks and the MCP server — lives in
**[matrixarkai/matrixark](https://github.com/matrixarkai/matrixark)**, also Apache-2.0.

Use this repository when you want the engine: embed the crate, run the binaries, or talk to a node
over RESP or HTTP. Use `matrixark` when you want the Python client, the extraction and retrieval
pipeline, or an agent integration.

## Deploy: laptop → replicated cluster

- **Local single node** — one Docker command (above); durable storage in a local volume, no
  metaserver dependency.
- **Distributed** — replicate through **MatrixRaft** consensus; the storage backend auto-resolves
  **object store → shared filesystem → replicated local disk**. Large raw content offloads to
  content-addressed, deduplicated storage while metadata stays in the index.
- **Serving modes, budgets, startup and recovery, storage resolution** are documented in
  [docs/benchmarks](docs/benchmarks/README.md) and the deploy manuals above.

## Architecture

```
Clients ── Redis (RESP) · HTTP · proxy · matrixark (Python)
   │
Engine ─ TemporalStore: temporal index · serving paths · append-structured page store
   │
Foundation ─ MatrixCache (multi-layer cache) · MatrixRaft (Rust Raft consensus)
   │
Storage backend (auto) ─ object store → shared filesystem → local + Raft
```

Five Apache-2.0 repositories, all public and open to anyone in the world to use, fork and
contribute to:
[TemporalStore](https://github.com/matrixarkai/TemporalStore) (engine) ·
[MatrixCache](https://github.com/matrixarkai/MatrixCache) (cache) ·
[MatrixRaft](https://github.com/matrixarkai/MatrixRaft) (consensus) ·
[MatrixObject](https://github.com/matrixarkai/MatrixObject) (object store) ·
[matrixark](https://github.com/matrixarkai/matrixark) (Python tooling).

---

## Build & test (from source)

```bash
cargo check -p temporalstore-rust --all-targets
cargo test  -p temporalstore-rust --lib --tests -- --test-threads=1
```

Focused harnesses:

```bash
cargo run -p temporalstore-rust --bin readiness_gate -- --service-reports
cargo run -p temporalstore-rust --bin context_workflow_harness
cargo run -p temporalstore-rust --bin storage_modes_harness
cargo run -p temporalstore-rust --bin raft_secondary_replication_harness
```

Formatting:

```bash
cargo fmt --all -- --check
```

## Before the first milestone: formats can change

Stored formats and wire shapes may still change between releases. The index carries a format stamp
and refuses an image it cannot read rather than guessing, so an upgrade fails loudly instead of
silently mis-reading a store. Pin a version for anything you care about keeping.

## Status & evidence

Apache-2.0. Read the project's production-readiness claims from passing reports rather than from this README:

- [Benchmarks: token & quality against full local replay](docs/benchmarks/README.md)
- [Context management on TemporalStore](docs/context_management_on_temporalstore.md)
- Deploy: [Windows Docker](docs/windows_docker_install.md) · [Linux](docs/linux_deploy.md) ·
  [macOS](docs/macos_deploy.md)

The engine is Rust-native end to end: the temporal index, the serving paths and the page store
are all Rust, with no foreign-language bridge anywhere in a request. Out of scope unless
separately re-added: alternate wire-protocol compatibility (**no brpc/thrift** layer, clients
speak RESP or HTTP), and live external object-store integration.

## Contributing

**Anyone is welcome to contribute, and you do not need to ask first.** If something is wrong,
unclear or missing, open an [issue](https://github.com/matrixarkai/TemporalStore/issues); if you
would rather fix it yourself, send a
[pull request](https://github.com/matrixarkai/TemporalStore/pulls). You need no connection to this
project and no permission: the licence is Apache-2.0 and the fork button works for everyone.

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) and [SECURITY.md](docs/SECURITY.md). Contributions
target `main`, this repository's default branch and the branch every change merges to. The
older `rust-main` name is retired and no branch by that name exists here, so a pull request
opened against it has nowhere to land.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE`](LICENSE), [`NOTICE`](NOTICE)).
Third-party dependency licenses and attributions are listed in
[`THIRD-PARTY-NOTICES.md`](docs/THIRD-PARTY-NOTICES.md). Product and crate names are trademarks
of MatrixArkAI; see [`TRADEMARKS.md`](docs/TRADEMARKS.md).
