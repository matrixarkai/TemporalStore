# Context Extraction And Injection Workflow

This document describes the Rust-native Context workflow used for local validation and model
provider switching. The implementation is inspired by OpenViking's hierarchical context idea, but
TemporalStore keeps Context data in its existing `ContextNode`, `ContextEvent`, `ContextIndexRef`,
`ContextPackAudit`, and `ContextSummaryDirtyMarker` models.

## Workflow

The workflow is:

1. `extract`: deterministic mock extraction creates a node, event, source index ref, and dirty
   marker from mocked data such as incidents, tickets, documents, chats, code snippets, or user
   events.
2. `normalize`: extracted context is split into OpenViking-style tiers:
   - `L0`: short routing summary
   - `L1`: structured key-fact summary
   - `L2`: full source payload reference
3. `persist/index`: the workflow writes Context model commands through `TemporalEngine`.
4. `retrieve/inject`: retrieval reads Context models by tenant/node/time/filter and injects selected
   blocks into a prompt under a token budget.
5. `audit`: injection persists a `ContextPackAudit` with selected and blocked refs.

## API

The data-node server exposes:

- `POST /context/extract`
- `POST /context/retrieve`
- `POST /context/inject`
- `GET /context/workflow/state`
- `GET /context/model/providers`
- `POST /context/model/provider`

Model provider config is OpenAI-compatible by shape:

```json
{
  "provider_name": "mock-openai-compatible",
  "provider_kind": "mock",
  "base_url": "",
  "api_key_env": "",
  "model": "mock-context-chat",
  "embedding_model": "mock-context-embedding",
  "timeout_ms": 30000,
  "max_retries": 2,
  "mock_mode": true
}
```

Open-source and commercial models should use the same config shape with
`provider_kind=open_ai_compatible`, an `http://` `base_url`, and `mock_mode=false`. The Rust-native
local path calls `/chat/completions` with bounded deadlines/retries, loads the bearer token only
from `api_key_env`, parses `choices[0].message.content`, and can fall back to a configured mock or
secondary provider if the live endpoint is unavailable.

## OpenViking Comparison

Adopted:

- layered L0/L1/L2 context
- URI-like stable refs
- provider-configurable model backends
- mockable local workflow

Different:

- TemporalStore stores data in Context models rather than a separate `viking://` filesystem
- refs use `tsctx://tenant/<id>/node/<id>/...`
- local validation is deterministic and does not require external model credentials

## Local Testing

Run the local harness:

```bash
tools/run_context_workflow_local.sh
```

Run the Docker-packaged binary after building the image:

```bash
docker build -t temporalstore-rust:context .
docker run --rm temporalstore-rust:context context_workflow_harness
```

The harness verifies:

- mocked extraction succeeds
- Context blocks are retrieved
- prompt injection includes `<context>`
- selected refs are recorded in `ContextPackAudit`
- the provider config is reported in the JSON summary

## Production Readiness

The production readiness gate now tracks this workflow as `context_workflow`.

Covered:

- Context models are persisted through the normal engine command path.
- L0/L1/L2 tier generation and prompt injection are deterministic for local mocks.
- OpenAI-compatible HTTP model execution supports bounded deadlines, retries, environment-backed
  bearer auth, JSON summary parsing, and fallback provider execution.
- Data-node HTTP routes expose extract, retrieve, inject, workflow state, and provider inspection.
- The local harness and Docker-packaged harness validate extraction, retrieval, injection, and
  audit refs.

Still blocking C++ parity and production readiness:

- C++/OpenViking golden context corpus replay through engine, client, proxy, Redis/admin,
  shared-store, and Raft paths
- production policy controls for PII filtering, tenant isolation, prompt-size admission, rate
  limiting, and provider failure budgets
