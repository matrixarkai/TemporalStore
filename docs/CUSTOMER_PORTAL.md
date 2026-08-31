# The customer portal

Seven pages served by the gateway itself, at `/v1/admin/*`. They exist so a customer running
MatrixArk as a managed service can configure it, prove it works, fill it, use it, and watch it —
without shell access to the host.

| Page | Path | What it is for |
| --- | --- | --- |
| Overview | `/v1/admin` | Where the deployment stands: a checklist of the things that fail quietly |
| Setup & metrics | `/v1/admin/setup` | Choose the models, store the provider key, tune 79 settings, probe the endpoints, read live traffic |
| Skills & resources | `/v1/admin/catalog` | List and filter what the deployment holds; read the stored text; retire a superseded skill |
| Explore | `/v1/admin/explore` | Run a real retrieve, add a memory, upload a document, browse what is stored |
| Ingestion | `/v1/admin/ingestion` | Import a directory of documents and watch the job |
| API keys | `/v1/admin/portal` | Create, list, rotate and revoke keys; per-key usage |
| API | `/v1/admin/api` | Every route this deployment serves, its scope, and a curl that runs |

Fetching any page needs no credentials. Every **action** on a page calls an authenticated endpoint,
so a page is inert without a key — the same posture the key portal has always had.

**Five of them are generated.** Overview, Setup, Skills & resources, Explore and API are emitted by
`tools/portal/build_portal_pages.py` so they share one stylesheet and one nav, and so adding a page
updates the nav on every other page in the same step. Do not hand-edit those five: change the
script and re-run it. `api_key_portal.html` and `ingestion_portal.html` are hand-maintained and only
have their nav block refreshed. `test_matrixark_portal_pages` copies the directory, re-runs the
generator in the copy, and requires every page to come out byte-identical.

---

## Overview

`GET /v1/admin/overview` (admin scope) returns the deployment's setup state as an ordered
checklist, computed server-side so the page makes one request and so the same answer is available
to anything scripting a deployment check.

Every item on it is something that **fails quietly**: an unconfigured extraction provider still
answers `200`, an unset ingestion root only surfaces when an import is refused, and an empty store
looks exactly like a store whose retrieval is broken. Each is phrased as the thing to do next
rather than as the flag that is off, and carries a link to the page that fixes it.

**Each unfinished item also carries `how`** — the steps, in order, for the state it is in.
"No model is configured" says what is wrong and leaves a customer to work out where the setting
lives, what the endpoint must look like, and which variable the key goes in; those steps are short
and known, so they travel with the finding. A `todo` shows them open, a `warn` collapsed, and an
item that is already green carries none: the useful thing about a green item is that there is
nothing to do. The configuration-warnings item's steps *are* the warnings, rather than a second
wording of them.

The report degrades rather than fails: if the backend cannot answer a listing, the counts come back
`null` and the configuration half is still returned — the moment an operator most needs the
checklist is when the backend is the broken thing.

**Diagnostics.** *Copy* or *download* assembles the checklist, the effective configuration and the
current `/v1/metrics` text into one JSON document — the thing to attach to a support request. It
carries only the settings that differ from their built-in default, because that is the whole
question when someone asks why this deployment behaves differently from another one, and it never
carries a secret: a key is reported as `<configured>` or absent.

---

## Setup

### Choosing the models

Two model roles, configured independently:

* **Extraction** — the LLM that turns ingested text into memories. DeepSeek, vLLM, Ollama and
  OpenAI all speak the same OpenAI-compatible shape. Set the provider to `openai_compatible`, give
  it a base URL **ending in `/v1`**, a model name, and a key.
* **Embedding** — the encoder that makes retrieval semantic. **DeepSeek has no embeddings API**, so
  a DeepSeek deployment pairs it with a local encoder (`tools/context_minilm_embed_server.py`) or
  another OpenAI-compatible embedding endpoint.

Presets for DeepSeek, OpenAI, Ollama and the local MiniLM encoder fill in the same fields you could
type by hand — they can write nothing the closed registry below does not already allow, and no
preset carries a secret.

### What is writable

79 settings across seven groups: extraction, embedding, retrieval and context budget, skills and
resources, ingestion pipeline, rate limits and quotas, and retrieval behaviour. The first four are
open by default; the rest start collapsed, because a customer standing a deployment up needs the
models and nothing else, and a wall of tuning knobs above the fold is how the two fields that
matter get missed. A filter searches label, help text and variable name at once, and opens whatever
group the matches are in.

**Twelve of the 79 are marked required**, and *only what I must set* narrows the page to them.
Everything else is a tuning knob with a defensible default; these have none that is right in
production, because the right value is a decision about your deployment — which model, whose key,
where the documents are. A page that presents all 79 as equals answers "what can I change" and
leaves "what must I set" unanswered, and the second is the only question a customer setting up
actually has. A test keeps the mark honest: a required key must exist, must not be a tuning knob,
and a secret is required only alongside the variable that names its destination.

The **retrieval behaviour** group is derived from `matrixark_tenant_policy.KNOBS` rather than
retyped. Those knobs already carry their type, default, choices and a written rationale — several
recording the measurement that explains the default. A second copy in the portal would be the copy
nobody updates. A per-tenant policy record still overrides any of them for that tenant; what the
portal sets is the deployment-wide fallback.

### What is not writable

Storage locations, cluster addresses, worker counts, bind addresses and the auth mode appear on the
setup page as a **read-only deployment inventory**. Repointing a storage directory or a metaserver
address from a browser form does not reconfigure a running deployment, it strands its data; turning
auth off from inside the authenticated surface is a hole with no upside. So they sit outside the
writable registry entirely rather than being editable fields with a warning label. A variable whose
name looks like it holds a credential is reported as configured/not rather than by value.

### Where a setting goes, and when it takes effect

Each setting drives one environment variable, named on the field, and is labelled **live** or
**needs restart**. That distinction is not cosmetic:

* The extraction **endpoint, model, timeout and max-tokens** are read once at import into module
  constants (`matrixark_mcp_core`, `matrixark_mcp_extraction_provider`). A write is stored and
  applied to the process environment, but the extractor already captured the old value.
* The extraction **API key** is read per call, so a new key is live immediately.
* The **rate limits and quotas** are read by `GatewayConfig.from_env`, which runs once at startup —
  syntactically per call, behaviourally frozen.
* Most **embedding** and **behaviour** settings are read per call, so those are live.

A save response names exactly which keys are waiting on a restart, and the page says so rather than
reporting a change as applied when it is not.

**This is verified, not asserted.** `test_matrixark_gateway_config_audit` walks the AST of every
module under `tools/`, finds each occurrence of an environment-variable name, and classifies it by
whether it sits inside a function or at module level — with an explicit list of startup-only
functions, and standalone server entrypoints excluded because they are separate processes. A
setting with an import-time reader must be labelled `restart`, and a setting labelled `restart`
whose readers are all per-call fails too. It caught 20 mislabelled settings and one phantom knob
that nothing read at all.

### Precedence

```
built-in default  <  runtime config file  <  environment exported by the launcher
```

which matches `matrixark_load_config`. Anything you `export` before starting the gateway keeps
winning over what the portal stored, so a portal write can never silently override a deliberate
deployment choice. Each field reports its source as `default`, `portal`, or `environment`.

An explicit portal write is different from a boot seed: it is an action taken now, so it applies to
the live process environment unconditionally.

### Storage and secrets

Settings live in `~/.matrixark/runtime_config.json` (override with
`MATRIXARK_RUNTIME_CONFIG_FILE`), written atomically with mode `0600`.

A secret is **write-only**. Its value is stored and pushed into the environment variable named by
the matching `*_API_KEY_ENV` setting — so a DeepSeek customer's key lands in `DEEPSEEK_API_KEY`,
which is the variable the provider code reads — but no read on this surface ever returns it. The
snapshot reports whether the key resolves, never what it is. Leaving a secret field blank on save
means *keep the stored one*.

Only the keys in `matrixark_gateway_config.SETTINGS` can be written. An unknown key is a `400`;
this is not a general environment-variable setter. A field can be reset to its built-in default,
which drops the stored override rather than storing an empty string.

### Moving a configuration

`GET /v1/admin/config/export` emits exactly what `POST /v1/admin/config` accepts, so "make staging
behave like production" is one request rather than 79 fields read off one page and retyped into
another. By default it carries only the settings that differ from a built-in default;
`?include_defaults=1` emits everything.

Secrets are **omitted, never blanked**. A blank is a write, so an import that "just applied
everything" would clear the target's working key and break the deployment it was meant to
configure. The response names what it left out, so the import always ends with "now set the key" —
deliberately.

The setup page exposes this as *export* (downloads a file), *copy as curl*, and a paste box that
applies one.

### What changed, and when

`GET /v1/admin/config` carries a `history`: the last 50 writes, newest first, each with who made
it, what it touched, and **what each setting changed from**. "Who changed the embedding model" is a
real support question, and a file that records only the latest state answers neither half of it.

The previous value is the *effective* one, so the first change after a deployment reads as
`exported-model → minilm` rather than as from-nothing when the old value came from the launcher
environment.

A secret is recorded by key only. That a key was rotated is exactly the useful part; the value is
exactly the part that must not be written down.

The log is bounded at 50 entries because this file is read on every boot and every admin read.

### Proving it works

`Test endpoints` calls the configured extraction and embedding endpoints once and reports status,
latency, model name, and for the encoder, the vector dimensions.

This step is worth doing every time, because **both paths fail silently**: extraction and embedding
each fall back to a local deterministic path when unconfigured or unreachable, and that path
answers `200` with rule-extracted memories and hash vectors. A rejected key and a working one are
indistinguishable at the API surface until something probes them.

### Endpoints

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| `GET` | `/v1/admin/overview` | admin | Readiness checklist, counts, traffic |
| `GET` | `/v1/admin/config` | admin | Effective configuration, warnings, the writable registry, the inventory |
| `POST` | `/v1/admin/config` | admin | Write settings (`null` resets one); response names what needs a restart |
| `POST` | `/v1/admin/config/preset` | admin | Apply a provider preset |
| `POST` | `/v1/admin/config/test` | admin | Probe the configured endpoints |
| `GET` | `/v1/admin/config/export` | admin | The configuration as a patch body, for another deployment |
| `GET` | `/v1/admin/scopes` | admin | What each scope permits, plus four key shapes |

```bash
curl -sX POST http://gateway:8080/v1/admin/config \
  -H "Authorization: Bearer $ADMIN_KEY" -H 'Content-Type: application/json' \
  -d '{"settings":{"extraction.provider":"openai_compatible",
                   "extraction.base_url":"https://api.deepseek.com/v1",
                   "extraction.model":"deepseek-chat",
                   "extraction.api_key_env":"DEEPSEEK_API_KEY",
                   "extraction.api_key":"sk-..."}}'
```

---

## Skills, resources and memory

`matrixark_list_skills` and `matrixark_list_resources` existed in the backend since the skill lane
landed, but were reachable only through `/v1/mcp` — so a customer on the documented REST contract
could ingest skills and had no way to see what was stored. `/v1/users` had a POST form and no GET,
unlike `/v1/memories`. These close both:

| Method | Path | Scope | Query / body |
| --- | --- | --- | --- |
| `GET` | `/v1/skills` | `skill:read` | `user_id`, `agent_id`, `session_id`, `limit`, `include_disabled` |
| `GET` | `/v1/resources` | `resource:read` | `user_id`, `agent_id`, `session_id`, `limit`, `resource_type` |
| `GET` | `/v1/users` | `context:retrieve` | `user_id`, `agent_id`, `session_id`, `limit` |
| `POST` | `/v1/skills/update` | `skill:manage` | `skill_hash` (required), `status`, `precedence`, `version`, `triggers`, `allowed_tools` |

The tenant is pinned from the authenticated key, exactly as on every other data route, so a key can
never read another tenant's catalog. `limit` is clamped at 500 at the edge — the adapter's listing
walks the record log, so an unbounded caller-supplied limit is a full-store scan.

`/v1/skills/update` is what makes the catalog usable rather than only readable: without it a
superseded playbook keeps competing for pack slots with its replacement, and the only way to retire
one was to edit the registry out of band. The catalog page exposes it as an Enable/Disable button
per skill, alongside a text filter over everything on the row and a resource-type filter.

Selecting a row on the catalog page reads the stored text through `POST /v1/resource/content` — the
chunks retrieval will actually draw from, not the source file on disk.

---

## Explore

Three tabs over the ordinary data contract, so a customer can exercise the pipeline against their
own data before pointing a workload at it:

* **Ask** — `POST /v1/retrieve` exactly as an agent would, showing each pack item with its layer,
  score and id, plus the token count and wall-clock latency. A retrieve that returns the right
  thing is the only check that covers ingestion, extraction, encoding and ranking at once. An empty
  result on a non-empty store almost always means the embedding provider is still deterministic:
  hash vectors do not match on meaning.
* **Add a memory** — `POST /v1/ingest` with one turn. With *extract immediately* off it behaves the
  way a production ingest does: acknowledged, extracted in the background, so a retrieve issued a
  moment later may not see it yet.
* **Upload a document** — `POST /v1/ingest_file`, which streams the file to the blob tier and
  ingests it in one call. Choose whether it is stored as a skill or a resource, and its visibility.
  For a whole directory, the Ingestion page is the right tool.
* **Browse** — `GET /v1/users`, `GET /v1/memories`, and a memory's record plus its change history.

Two things this surfaced, both fixed:

* `X-Scope` on `/v1/ingest_file` is documented as a JSON scope object and was passed through
  unparsed. It reached the string branch of `_apply_identity`, which reads a bare string as a
  *namespace label* — so an upload with `X-Scope: {"user_id":"alice"}` was filed under namespace
  `acme/{"user_id":"alice"}` and the user_id was dropped. Nothing errored; the file simply landed in
  the wrong scope, which only shows up later as a retrieve that cannot find it.
* An unreachable blob tier was the one failure on that route that escaped as an unhandled
  exception — a bare `500` and a stack trace that reads like a bug in the upload rather than a
  backend that is down. It is now a `502 blob_store_unreachable` naming the URL it could not reach.

---

## Scopes

`GET /v1/admin/scopes` (admin scope) returns what every scope permits, plus four ready-made key
shapes: **agent** (write, read, rate — no deletion, no management), **read-only**, **ingest-only**,
and **admin**. The key portal renders them as presets over a checklist that says what each scope
allows, kept in two-way sync with the raw comma-separated field for anyone who prefers to type.

The form previously asked for scopes as free text and explained none of them, which produces one
outcome everywhere it appears: people paste the docs example, which is wider than they need,
because guessing narrow and being wrong costs a debugging session and guessing wide always works.

The catalogue is pinned to `MATRIXARK_TOOL_SCOPES` by a test in both directions. A scope the
backend enforces and the catalogue does not describe is one a customer cannot discover — the form
would never offer it, and a key issued without it fails with a `403` naming a string that appears
nowhere in the portal. A scope described here that nothing gates on is one they could grant to no
effect and then trust. The test caught `admin:sso` missing on the first run.

---

## Ingestion progress and retry

### Watching an import

A job card shows the document it is on and how long it has been there, a bounded ring of what it
just processed with the slowest called out, and a failure table: each document, why it failed, and
whether that failure is worth retrying. On a long import the counters barely move and the current
file changes too fast to read — the recent ring is the part that shows it is working, and which
documents are slow.

`failures_truncated` is surfaced rather than silent. A job that fails more documents than it keeps
a record of can only be retried for the ones it listed, and a retry button that quietly covered the
first 500 of a larger problem would be worse than no button.

### Seeing it from the landing page

`GET /v1/admin/overview` carries an `imports` summary, and the overview shows a live strip while
anything is running: percentage, the document it is on, and an ETA. Across concurrent imports the
ETA reported is the one that finishes **last**, not the sum — summing them would claim an import
takes as long as all of them together, which is only true if they run one after another.

Failures waiting for a retry become a checklist item of their own. An import that ended with work
outstanding is otherwise a state you only find by opening the Ingestion page, which is not where
anyone starts.

The page paces itself from what it finds: four seconds while an import is moving, thirty when
nothing is, and nothing at all from a hidden tab.

### Retrying

`POST /v1/admin/ingestion/jobs/{id}/retry` resubmits **only the failures**, and by default only the
ones worth retrying. Ingest is a keyed upsert, so re-running the whole directory is *safe* — which
is exactly why it is what people do, and why a thousand-document import with three failures gets
re-run in full and ends with the same three failures. The two jobs are linked in both directions,
so the history reads as one import.

A failure is worth retrying when it is a timeout, a refused connection, an exhausted 5xx, `429` or
`408` — the deployment having a bad minute. It is not when it is any other 4xx, or a file that
would not open: the request itself is wrong and will be wrong again. Pass `only_retryable=false` to
resubmit everything anyway.

Writing that classifier found a real defect. `post_document` gave up immediately on any 4xx —
correct for a `400`, and exactly backwards for a `429`. The gateway **rate-limits ingest** and
answers `429` with a `retry-after` header, so a large import against a busy deployment was
abandoning precisely the documents it had been asked to send later; the job reported 1000 documents
and the store held fewer. Both are now retried, honouring `retry-after` where the server sends one,
capped so one header cannot park an import.

`matrixark_ingestion_documents_retryable` is a gauge: non-zero after an import means work is
waiting for someone to press retry, which was otherwise visible only to whoever opened the page.

### Uploading from the browser

Explore's upload takes several files and sends them one at a time, so a failure stops at the file
it failed on rather than becoming one line in a pile of parallel errors. Each file shows real byte
progress — `fetch` reports nothing until a request finishes, so this uses `XMLHttpRequest`, which
still has the only upload-progress event there is — and then *extracting* once the last byte is on
the wire, which is the difference between "stuck at 100%" and "nearly there".

A failed upload keeps its `File` and offers **Retry**. That retry works only while the tab is open,
and the page says so: the bytes came from the browser and were never on the server, so nothing else
could resend them. A bulk import from a server directory is the retryable-after-a-restart path.

---

## Live updates

`GET /v1/admin/events` (admin scope) is a server-sent event stream carrying this deployment's live
state: traffic counters, imports in progress, the encoding backlog and the configuration-warning
count. The overview and setup pages read it and show **live** in the corner.

It replaces polling. Three pages were each running their own timers against three endpoints, so an
import that finished between two polls left a stale bar on screen until the next one, and a page
left open cost three requests every few seconds whether or not anything had changed.

Design notes worth keeping:

* **Read over `fetch`, not `EventSource`.** `EventSource` cannot set an `Authorization` header, and
  the alternative is the key in a query string — where it lands in every access log and proxy trace
  between the browser and the gateway. Same wire format; only the reconnect is ours to write, with
  exponential backoff so a gateway that is down is not hammered.
* **`X-Accel-Buffering: no`.** Nginx buffers a proxied response by default, which turns a live
  stream into a single delivery when it finishes. This one header is the difference between live
  and not.
* **The expensive part rides slower.** Everything in a frame is read from memory except the
  encoding count, which walks the record log — that refreshes every 30 seconds, not every tick.
* **A stream is not a latency.** `/v1/admin/events` is excluded from the request-duration
  histogram: a subscription lasting ten minutes left in there gives a p99 of ten minutes,
  describing nothing anyone waited for. The request and its bytes are still counted.
* **Bounded.** A stream closes after ten minutes and the browser reconnects, so an abandoned tab
  does not hold a connection for the life of the worker.

---

## Encoding progress

Ingest can defer encoding: chunking is synchronous and the vector is filled in behind it. Between
the write and the drainer catching up, a chunk **exists and cannot be matched on meaning** — so a
retrieve over that window returns less than it should and says nothing about why. That reads as
"retrieval is bad" rather than "retrieval is not finished yet", and the difference is a number
nobody was counting.

`GET /v1/admin/embeddings` (admin scope) answers it: how many vectors are encoded, how many are
waiting, the models and vector widths in use, and any deferred pipeline work still to run. Setup
shows it as a bar that moves — fast while there is a backlog, because watching it shrink is the
point, and slowly once there is not. The overview surfaces a backlog too, next to a running import:
both are "the deployment is still working on what you gave it", and both were invisible from
anywhere else.

Three things it reports that a bare count would miss:

* **The configured encoder travels with the counts.** With no encoder configured, nothing is
  waiting *because nothing will ever be encoded* — every vector is a hash fallback. A bare "0
  pending" would read as "all done".
* **Mixed vector widths.** Vectors of different dimensions cannot be compared, so a store holding
  two widths is a store where some memories can never match a query. That happens the moment the
  embedding model changes without a backfill, and looks exactly like ordinary poor recall.
* **A backend that cannot answer is an error, not an empty backlog.** "Nothing is pending" and "I
  could not find out" must not look the same: the first says retrieval is ready.

The count uses its own pending predicate rather than the retrieval one. `is_pending_async_candidate`
answers "is this candidate a placeholder I should rank differently" and returns `False` unless
`ref_type == "event"`, because an event is the only shape retrieval ranks — so a backlog counted
with it omits every pending embedding for a resource chunk or a skill section, which is most of what
a bulk import produces. A backlog must never read smaller than it is.

**Deliberately not on `/v1/metrics`.** Answering this walks the record log; a scrape doing that
every fifteen seconds is a self-inflicted load on the store it is meant to be watching.

---

## Metrics

### The scrape

`GET /v1/metrics` — Prometheus text, no credentials. Aggregate counters only: no keys, no key ids,
no tenant or user identifiers. Per-key usage stays behind the admin-gated
`GET /v1/admin/api_key_usage`.

```yaml
scrape_configs:
  - job_name: matrixark_gateway
    metrics_path: /v1/metrics
    static_configs:
      - targets: ['gateway:8080']
```

### What it exports

| Metric | Type | Notes |
| --- | --- | --- |
| `matrixark_gateway_requests_total{route,method,status}` | counter | Edge requests |
| `matrixark_gateway_request_duration_seconds{route}` | histogram | Edge latency |
| `matrixark_gateway_request_bytes_total{route}` | counter | Body bytes accepted |
| `matrixark_gateway_response_bytes_total{route}` | counter | Body bytes served |
| `matrixark_gateway_requests_in_flight` | gauge | Concurrency right now |
| `matrixark_gateway_embedding_semantic` | gauge | `0` = retrieval is on hash vectors |
| `matrixark_gateway_extraction_model_active` | gauge | `0` = ingest calls no model |
| `matrixark_gateway_config_warnings` | gauge | Configuration problems, count |
| `matrixark_gateway_config_changed_timestamp_seconds` | gauge | When the stored configuration was last written; `0` if never |
| `matrixark_gateway_settings_overridden` | gauge | Settings whose value is not the built-in default |
| `matrixark_ingestion_documents_retryable` | gauge | Failed documents worth retrying; non-zero means work is waiting |
| `matrixark_ingestion_documents_retried` | counter | Documents submitted as part of a retry |
| `matrixark_ingestion_*` | counter/gauge | Bulk import registry |

The `route` label is a **route template**, not the request path: `/v1/memory/abc123` and
`/v1/blob/resources/ab/deadbeef` collapse to `/v1/memory/{id}` and `/v1/blob/{key}`, and a path
nobody recognises becomes `other`. Paths carry customer-chosen identifiers, so labelling by raw path
would let any client create unbounded series in the operator's Prometheus.

### Grafana

The dashboards and alert rules are **served by the gateway** at
`GET /v1/admin/monitoring/{gateway|ingestion|alerts}`, and the setup page offers them as downloads.
The portal used to name a repo path, which a customer running this as a managed service cannot
reach — and even with a checkout, the file on their disk is whatever their copy is rather than what
this build emits, which is exactly the drift the dashboard test exists to prevent.


* `docs/ops/matrixark-gateway-dashboard.json` — config health, request rate, errors by status,
  p50/p95/p99 latency, byte throughput, ranked routes, plus when the configuration last changed,
  how many settings are away from stock, and worker uptime.
* `docs/ops/matrixark-ingestion-dashboard.json` — bulk import progress and backlog.
* `tools/temporalstore-prometheus/matrixark-gateway-alerts.yml` — alert rules.

Half of "it started behaving differently on Tuesday" is answered by knowing whether anyone changed
the configuration on Tuesday. The change timestamp puts that on the same dashboard as the effect;
it carries the fact and the time, never a value.

**The dashboards and the metrics are held together by a test.** Both directions fail silently
and neither fails loudly: a panel querying a series nobody emits is a **blank panel**, which on a
monitoring dashboard reads as "no traffic" — the most misleading thing it can say. And a metric
emitted and charted nowhere is work nobody sees. `test_matrixark_dashboards` compares the two,
requires every panel to carry a description, and checks no panels overlap. It caught six emitted
metrics shown nowhere, including the two added precisely because the state they describe is
invisible otherwise: documents waiting for a retry, and when somebody last changed the
configuration.

The two gauges to alert on are `matrixark_gateway_embedding_semantic == 0` and
`matrixark_gateway_extraction_model_active == 0`. They are the only signals that catch a silently
misconfigured deployment: every other panel — request rate, error rate, latency, storage — looks
healthy while retrieval runs on hash vectors.


---

## Accessibility

Every outcome on these pages lands by replacing the contents of a `div` — a save confirmation, a
probe verdict, an upload failure. Focus never moves, so without help none of it is announced and
the page silently does something. Those regions carry `role="status"` and `aria-live="polite"`, and
the Explore tab strip carries the `tab`/`tabpanel` roles that its `aria-selected` implies.

The shared `--faint` token carries the help text under every field, which makes it body copy rather
than decoration. It was 3.85:1 in dark and 3.04:1 in light against the panel it sits on — below AA
for normal text, on exactly the prose that explains what each setting does. It is now 5.76:1 and
4.95:1. A lighter grey looks tidier and puts it back under 3:1, so the reason is written next to
the token.

Both schemes are checked at 375 px with no horizontal overflow on any page.


---

## Ingestion

Bulk import resolves every path inside `MATRIXARK_INGESTION_ROOT`; with it unset, submitting
server-side paths is refused outright rather than defaulting to the whole filesystem. The page now
says so at the top, with a link to the setting, instead of letting the first sign be a refused
submission after the operator has typed out a directory and a glob. Single documents can be
uploaded from Explore regardless.


---

## The API page

`/v1/admin/api` renders `GET /v1/admin/routes` — 45 routes across Health, Memory, Catalogue, Blobs,
Administration and the portal pages, each with the scope a key must carry and a `curl` built against
the host you are reading it on. The key is left as `$MATRIXARK_API_KEY` rather than pasted in, so a
copied command is never a copied credential.

Neither the page nor the JSON needs credentials: it is the published contract, and every route it
names enforces its own access.

Two tests keep it honest, and both are about the same failure — documentation that drifts is worse
than none, because it is trusted:

* **The route set is compared to the source, in both directions.** The test parses the gateway's
  own path literals and `_DATA_ROUTES` table. A route that exists and is undocumented is one a
  customer cannot find; a documented route that no longer exists is one they will write against.
  It caught `/v1/admin/routes` itself on the first run.
* **Every example is executed.** Each documented request is driven through the app and must answer
  2xx. A published example that `400`s is copied first and read second, and the reader concludes
  the route is broken rather than the sample. This caught `/v1/memory/by-key`, whose example had no
  `identity_key` — required, so the offered command could only ever fail.
