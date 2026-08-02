# MatrixArk MCP Server

MatrixArk exposes context infrastructure to AI agents through MCP. This lets
Codex, Claude, Cursor-like products, or vertical agents call MatrixArk as a
context provider without depending on the model to manually remember event
ingestion.

The local MVP server is:

```bash
python3 tools/matrixark_mcp_server.py \
  --event-log /tmp/matrixark-mcp-events.jsonl
```

It is dependency-free and speaks MCP JSON-RPC over stdio. The current adapter
stores a JSONL event log for local testing; the adapter boundary is where
TemporalStore RPC calls should be added for production.

For the full public HTTP API, shared schemas, MCP tool schemas, and internal
TemporalStore context APIs, see `MATRIXARK_API_REFERENCE.md`.

For shell debugging, add `--line-json` and send newline-delimited JSON-RPC. MCP
clients should use the default stdio `Content-Length` framing.

## Tools

```text
matrixark_ingest    messages + scope + metadata + optional agent_hook
matrixark_retrieve  raw query -> ContextPack-like selected refs
matrixark_feedback  final answer confirmation/correction/rejection
matrixark_replay    replay captured events for debugging
```

## Minimal Payloads

Only these fields are required:

```json
{"messages": [{"role": "user", "content": "Alice approved the GPU request."}]}
```

```json
{"query": "what GPU approvals are current?"}
```

```json
{"messages": [{"role": "user", "content": "Yes, that answer is correct."}]}
```

Everything else is optional. Optional fields improve scope, quality, replay, or
automation, but they are not required for the MCP call to work:

```text
scope.user_id          optional user memory scope
scope.session_id       optional thread/run scope
scope.team/project     optional tenant/team/project routing hints
metadata.source        optional source label
metadata.node_path     optional tree/path hint; MatrixArk can choose internally
metadata.reply_to_*    optional feedback linkage
agent_hook             optional; only present when a host integration auto-captures
context_pack_id        optional but strongly recommended for feedback inference
accepted/rejected_refs optional feedback evidence
max_context_tokens     optional; retrieve defaults to 2048
raw_uri/resource_type  optional; only for resource ingestion
```

The public tool contract follows the Mem0-style shape: agents send messages,
scope ids, optional metadata, and optional hook evidence. MatrixArk does not ask
callers to send `ContextEvent`, `ContextEntity`, `ContextIndex`, or summary
records. Those are internal TemporalStore serving models.

MatrixArk always runs extraction and canonicalization. The extraction provider
can be deterministic local logic, OSS models, OpenAI/provider models, or
agent-provided hints in `metadata.agent_extraction`; MatrixArk validates and
normalizes all of them into its internal data model.

For short feedback such as `yes`, `approved`, or `wrong`, confirmation requires
prior context. The caller should provide `context_pack_id`,
`metadata.reply_to_context_pack_id`, accepted/rejected refs, or a stable
`scope.session_id` with previous messages. Without that, MatrixArk stores the
message but returns `AMBIGUOUS`.

For local MVP extraction, MatrixArk now pulls a bounded prior-message window for
extraction when prior context exists, including ordinary messages, business
events, resources, and feedback:

```text
1. explicit context_pack_id / reply_to_context_pack_id -> ContextPack summary + refs
2. same-session summary for the same context node first, then raw prior events within budget
3. same-user summaries/entity state first, then raw prior events with a warning
4. no prior context -> extract the new event alone; short feedback stays AMBIGUOUS
```

The prior window is capped to 8 records and 4 KB of text. MatrixArk records
`prior_refs`, `prior_message_count`, and the prior-context level so replay can
show which session summaries, ContextPack refs, and raw prior messages were used
for extraction. Raw messages are included only until the extraction budget is full.
For normal messages, prior context improves extraction but is not mandatory. For
ambiguous feedback such as `yes`, `approved`, or `wrong`, prior context is
required for confident confirmation/correction. Production providers can replace
the rules-first classifier with OSS/OpenAI extraction while keeping this bounded
retrieval and replay contract.

The local MCP MVP also exercises the serving-store path for summaries and
embeddings:

```text
ingest message or hook event
-> normalize envelope
-> pull bounded prior context when available
-> write ContextSummary-style node summaries
-> write ContextEmbedding-style node and event embeddings
-> write ContextEvent
-> retrieve query embedding
-> score node summary embeddings per path layer
-> score event embeddings and keyword overlap
-> write ContextPackAudit with summary, refs, layer_scores, and replay evidence
```

The local adapter stores these records in the JSONL event log as stand-ins for
TemporalStore records. Embeddings use a deterministic token-hash encoder so tests
are dependency-free; production should replace that encoder with the configured
OSS/OpenAI/provider model while keeping the same TemporalStore storage boundary.

The minimum useful scope is either `scope.user_id` or `scope.session_id`. Sending
both is best: `user_id` supports long-term user memory, while `session_id`
supports precise thread/run grouping. If `session_id` is missing, MatrixArk falls
back to `scope.user_id` for user-level memory lookup and returns a quality
warning when that fallback is used for confirmation/correction inference. Hook
integrations should auto-capture the host agent's native user and thread/run ids
whenever available.

## Codex Config Example

Add this to a trusted project `.codex/config.toml` or user `~/.codex/config.toml`:

```toml
[mcp_servers.matrixark]
command = "python3"
args = [
  "tools/matrixark_mcp_server.py",
  "--event-log",
  "/tmp/matrixark-mcp-events.jsonl"
]
startup_timeout_sec = 10
tool_timeout_sec = 60
enabled = true
```

For production, run a packaged MatrixArk MCP binary or container instead of the
repo-local Python script.

## Hook Capture

Agents can call the MCP tools manually, but vertical Cursor companies should
prefer automatic hooks around the agent loop:

```text
before_llm       capture user query and local-context hints
after_llm        capture final answer, selected refs, rejected refs, commitments
tool_result      capture tool input/output and status
resource_added   capture uploaded file, URL, PDF, markdown, or runbook reference
feedback         capture explicit user confirmation/correction/rejection
session_commit   batch commit any missed messages at thread end
```

Each hook is sent as `agent_hook` beside the normal MatrixArk envelope:

```json
{
  "messages": [
    {"role": "user", "content": "Alice approved the GPU request."}
  ],
  "scope": {"team": "infra_team", "project": "project_1"},
  "metadata": {"source": "cursor"},
  "agent_hook": {
    "source": "matrixark-sdk",
    "hook_type": "before_llm",
    "hook_id": "hook-before-gpu",
    "observed_at_ms": 1781500000000,
    "idempotency_key": "gpu-approval-1",
    "trigger": "user_message",
    "auto_captured": true
  }
}
```

## Validation

Run:

```bash
python3 -m unittest tools.test_matrixark_mcp_server
python3 tools/run_context_pipeline_scale_e2e.py --events-per-lane 5
```

The unit test validates MCP protocol shape, tool listing, hook-captured ingest,
retrieval, and ambiguous confirmation handling when short feedback lacks prior
context.
