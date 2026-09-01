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

Both model names are free text on the wire, and a name that is merely misspelt does not fail
loudly: extraction falls back to the local rules and embedding to hash vectors, and both answer
`200`. So the **Models** section offers a picker instead of a text box. `GET /v1/admin/models`
(admin scope) returns three things per role:

* the **measured** encoder catalogue — hit@1, hit@5, texts/s, MB/doc, parameters and window, from
  a run over a real corpus — rendered as a table sorted by hit@1 with the best in each column
  marked. It replaced a list written from general knowledge, and the two disagreed about the thing
  being chosen: the hand-written one omitted the whole e5 family, which measured best, and
  recommended the encoder the measurement puts *fifteen points of hit@1* below e5-small at
  identical size and memory. Two lists of one thing do not stay agreed, so there is one, and a test
  asserts the picker serves it rather than a copy;
* what the configured endpoint says it **actually serves** — an OpenAI-compatible server answers
  `GET <base>/models`, so asking it turns a guess into a choice. This runs only when asked
  (`probe=1`), because it is a request against the customer's own provider;
* for embeddings, what the **stored vectors were made with**, which is the fact that decides
  whether a change is safe.

Choosing fills the field below and leaves it unsaved, next to whatever else is pending — a model
change saved on click is one nobody reviewed.

#### Changing the embedding model

This is the one setting on the page that can silently invalidate data you already have. Changing it
does not re-encode anything: existing vectors stay in the old model's space, vectors from two
models cannot be compared, and those memories stop matching queries.

The trap is not a width change — a width change at least has a width to notice. Two encoders are
frequently the **same width**: `all-MiniLM-L6-v2` and `paraphrase-multilingual-MiniLM-L12-v2` are
both 384, `BAAI/bge-m3` and `voyage-3` are both 1024. Nothing in the stack sees a mismatch to
complain about. The portal therefore states each encoder's width and names the others it collides
with, derived from the catalogue rather than written into prose — a note only warns about the
collisions whoever wrote it thought of, and the first draft of that note called out the 384 pair and
said nothing about the 1024 one.

The warning is shown only when the choice would actually strand something: never when the store is
empty, and never when you are picking the encoder the store was already written with. A warning on
every render is one people learn to click past.

**A repository prefix is not part of a model's identity.**
`sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` and
`paraphrase-multilingual-MiniLM-L12-v2` are one encoder, and both forms occur in real configuration
— a store written under the short name, a catalogue offering the full one. Comparing the strings
exactly would decline every stored vector over a rename that changed nothing, which is the guard
causing the outage it exists to prevent. Names are compared on their final path segment; case stays
significant, because these are identifiers a loader resolves, not prose. Two genuinely different
models still conflict, including two from the same publisher.

#### The engine knew; nothing else did

The guards above live in the Rust engine, and there are two retrieve paths over the same store.

Against a TemporalStore backend the engine assembles the pack and Python packing is refused
outright, so the engine's guards apply. Against the **local** backend — the default
`MatrixArkLocalAdapter`, what an OSS install and every local deployment run —
`native_context_pack()` returns `None` by construction and retrieval is the Python adapter's own
scan. That path never consulted the encoder at all, so on it an encoder swap stayed exactly as
undetectable as it had been in the engine before it was fixed.

A test pins that claim rather than leaving it as prose: it asserts the local adapter returns no
native pack and does not require one, which is what makes the Python scan the live path there.

Two things made it worse there than a plain zero would be. Python's `cosine` is a bare dot product,
so a vector from another space returns a *plausible* number rather than nothing, and a plausible
number ranks. And the blend is `0.72 × (dense + 1) / 2 + 0.28 × sparse`, so a dense score of 0.0 is
worth 0.36 — a meaningless dense score is not neutral, it is a subsidy paid to the record for
carrying a vector nobody can read.

The identity was already there. Every vector-bearing record carries `embedding_meta` with the model
that wrote it — 69 of 69 in a freshly built store — because the separate embedding row was folded
into its owner and its fields ride along. Nothing read them. The retrieve path now declines a vector
whose encoder is not the active one and does not consult it at all, so the record follows the same
path a not-yet-embedded record follows. Counted separately from a width conflict, because two widths
means a provider outage seeded fallback vectors and two encoders at one width means the model was
changed: same symptom, different fix.

The count leaves on the pack and as a quality warning, so **Explore → Ask** says why an answer is
thin next to the thin answer, rather than leaving it to look like ordinary poor recall. When nothing
matches at all, the empty state names the encoder change when that is the cause instead of blaming a
deterministic provider — two causes that look identical from the outside, and naming only one sends
the reader to check a setting that is already right.

#### What the encoding panel was counting

`embedding_status` — which the encoding panel and the model picker's change warning both read —
skipped every record whose type was not `context_embedding`, and current ingest writes none of
those. On a store holding 97 vectors it answered `total: 0`, no models, no dimensions. The panel
renders that as *nothing stored yet*, and the picker turns it into *nothing a change could strand*:
wrong in the reassuring direction, about the one operation that silently invalidates data.

It now counts every record that carries a vector, keyed by what the vector belongs to so a log
holding both the retired row and its owner counts it once. A test pins its list of record types to
the retrieve path's, because a type in one and not the other means the panel describes a store
retrieval does not search.

Behind it, three guards in the engine make a change survivable rather than silent. All three were
broken and none had a test, which is how they came to be broken; each now has one, and each test
was checked by breaking its guard again on purpose:

1. similarity **refuses** mismatched vector lengths. It used to score the shared prefix, which
   returns a perfectly plausible cosine across two unrelated spaces — worse than no number, because
   it ranks;
2. ingest and the background drainer hash the **same** thing. Ingest hashed `provider.model`, the
   *chat* model, so the two paths recorded different hashes for the same encoder and neither pair
   could be compared;
3. retrieve **skips** vectors whose recorded encoder differs from the active one, scoring nothing,
   so the node falls through to the hybrid lexical pass exactly as an un-embedded node does. A hash
   of `0` means *unknown*, not foreign, and is still scored — every store written before the field
   existed carries `0`, and refusing those would turn an upgrade into silently dark retrieval.

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

Five tabs over the ordinary data contract, so a customer can exercise the pipeline against their
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
* **mem0 API** — a console over every method of the mem0 API, listed by the name you call it by
  rather than by the URL that serves it. See below.
* **Batch ingest** — many memories in one submission, run as a server-side job. See below.

### The mem0 console

A customer arrives holding mem0 code — `add`, `search`, `get_all`, `update`, `delete_all` — and the
route list answers a different question, because it is organised by URL. The console is the mapping.
Each operation renders its own form, shows the request before it is sent and the raw answer after,
and offers the equivalent `curl` — which names `$MATRIXARK_API_KEY` rather than carrying the key,
because that text lands on a clipboard and often in a ticket.

The forms are generated from one table, `MEM0_OPERATIONS` in the gateway, so an operation that
gains an argument gains a field. Two tests hold the table to the routes in both directions: an
operation naming a route that does not exist is a button that 404s, and a memory route no operation
reaches is a capability nothing in the portal can exercise. A third asserts that an operation's
declared scope matches what the gateway actually gates on — printing a scope next to the button
that disagrees with the enforcement sends a customer to issue the wrong key and read a `403` naming
a third thing.

The three `context:forget` operations are marked destructive from the route's own scope rather than
by hand, and a destructive operation will not run until its name is typed into a confirm box. The
confirmation is cleared after a successful run, or the next click repeats it against a box that
still says the magic word.

### Batch ingest

Each line is one memory: plain text, or a JSON object carrying a per-line `user_id`, `agent_id`,
`session_id`, `identity_key`, `role` or `metadata`. A pasted dump usually already says whose memory
each line is, and filing them all under one subject would be silent and unrecoverable without
re-ingesting, so a per-record identity overrides the batch's.

`POST /v1/admin/ingestion/records` runs it as a **server-side job**, not a loop in the tab. A
browser loop over ten thousand records is one closed laptop away from a half-finished import nobody
can resume or even count. As a job it inherits everything the directory import already had: a
background worker, progress, failure classification, retry of only the retryable failures, cancel,
and the metrics. The job registry's items are now either a path or a record, and a record carries
its own text so a retry can re-send it — a failure list keys by label, and a label cannot be turned
back into a record.

Blank records are dropped and counted rather than queued: an empty record fails identically every
time, so letting it into the job would fill the failure list with entries no retry can ever clear,
and a retryable-looking failure that never succeeds is how a retry button stops being trusted.
`empty record` is classified permanent for the same reason.

**Check what would be sent** resolves and counts without ingesting, so a paste can be confirmed
before it is committed to. The batch is capped at 8 MB and 20,000 records — both well above a
plausible paste, and past either a directory import is the right tool.

Two things Explore surfaced, both fixed:

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

## Retrieval settings: three layers, all reachable

Configuration sits at three levels, and until now the portal could reach only the outer one.

| level | what it covers | how many knobs |
|---|---|---|
| deployment | every tenant on this process | 79 settings, on Setup |
| **tenant** | everyone in one customer | **all 32 policy knobs** |
| **user** | one person | **12** — the read-path ones |

Deployment-wide config was settable and per-user is now, but **per-tenant was reachable only by
hand-editing the policy file** — so a multi-tenant deployment could not give one customer different
retrieval behaviour from another through the portal at all. `POST /v1/admin/policy` takes a `level`
of `tenant` or `user`; `GET /v1/admin/policy?user_id=alice` returns every knob with its effective
value, **which layer supplied it**, and which levels may set it. The tenant always comes from the
key, never from the request — otherwise a caller reads or rewrites another tenant's settings by
naming it.

Every deployment setting that a policy knob reads now says so on the field itself — *tenant can
override* or *tenant/user can override* — derived from the knob registry rather than written into a
group note. It was a note on one group, and the knobs are spread across two: the three skill knobs
said nothing, and those three are exactly the ones a single user can set. A field that does not say
it is a fallback reads as the final answer.

**A tenant may set the write-path knobs; a user may not.** A tenant owns its whole store, so
deciding there is a decision somebody can actually make. What no level can do is un-write records
already stored under the old value, so a tenant-level change to one of those says so — once, on the
change that has that property, rather than on every change.

Both levels persist into the same file, through one writer, so the two cannot drift on the edge
cases: temp file and rename, a missing file started fresh, a corrupt one left alone.

Resolution is **user → tenant → environment → built-in default**, through the same typed registry
all three already used. A second store keyed differently would be a second thing to keep correct,
and the two would drift on the edge cases the first one already handles.

### What may vary per user, and what may not

Not everything. A store is shared inside a tenant, so a per-user override that changes what gets
**written** leaves one store holding records of two shapes — and unlike a setting, that does not go
back when the setting does. It is the same failure as two writers disagreeing about the encoder:
nobody notices until retrieval is quietly worse for everyone.

So a knob declares the layer it belongs to, and **the default is `write`** — a knob added later
without anyone considering this is refused per-user until someone says otherwise. Refusing a
legitimate knob is visible and harmless; allowing a damaging one is neither. Twelve of the
thirty-two are read-path today: the candidate ceilings, sibling-session traversal, pack de-duplication
and the skill budget.

`recall_reinforcement` is the one worth naming. It reads like a retrieval knob and it is a
**writer** — measured, five searches produced 572 protection markers, about 114 writes per query —
so per-user divergence there changes what survives pruning for the whole tenant. It stays
tenant-level, and it is the reason the list was written by reading each knob rather than by
pattern-matching names.

A refusal names the knobs it refused rather than saying "some settings were rejected", and the
refusal sits at the layer rather than at the API: a write-path knob hand-written into the policy
file is dropped exactly as one submitted through the portal is.

### Identity is the pair

Per-user policy is keyed by **(tenant, user)**, never the user id alone — `alice` at two companies
is two people, and a policy keyed on the bare id would serve one customer's settings to the other.
A record carrying only half an identity is not stored at all. Both the human id and the scope hash
answer to the same policy, because a served record usually carries only hashes.

### Applied now, saved for later

A change takes effect on the next request — no restart, no reconnect, and no effect on any other
user. It is also written into the same policy file the tenant policy already uses, under a `users`
section keyed tenant-then-user:

```json
{
  "defaults": { "top_k_per_layer": 200 },
  "tenants":  { "acme": { "top_k_per_layer": 100 } },
  "users":    { "acme": { "alice": { "top_k_per_layer": 12 } } }
}
```

Written through a temporary file and renamed into place, so a process that dies mid-write cannot
leave a fragment the loader would serve as its "last good" copy. A file that is *corrupt* is never
overwritten — the loader is still serving the last good version, and replacing it would destroy the
only record of what was configured.

When no policy file is configured there is nowhere to write, and the response says so rather than
letting *applied* read as *saved*: a change that quietly vanishes on the next deploy is worse than
one that was never offered, because the customer has stopped thinking about it.

### One registry, checked

Three knobs were defined twice — `top_k_per_layer` (24 then 240), `max_global_candidates` (2048 then
20480), `max_selected_refs` (1000 then 10000). A dict comprehension keeps the last, so the earlier
definitions were dead: editing one changed nothing and reading one to learn the default gave the
wrong number. 35 call sites, 32 knobs. The duplicates are gone and the registry now raises on a
repeated name instead of silently keeping whichever came last.

---

## Which engine answered, and how it ranked

One store, two implementations that rank differently:

| path | when it serves | how it orders results |
|---|---|---|
| Python local adapter | the local backend — the default `MatrixArkLocalAdapter`, what an OSS install runs | dense cosine blended with lexical overlap |
| native Rust pack | a TemporalStore backend, where Python packing is refused outright | term overlap plus boosts — **no vector is read** |

Nothing in the response used to say which one answered, so the same query against two deployments
gave materially different results with no way to tell why. Worse, the Setup summary card described
retrieval as *semantic* or *hash vectors* from the embedding **provider** — a configuration value.
Whether an encoder is configured and whether the path that answers consults it are different
questions, and on one of these paths the answer is no.

Every pack now carries `served_by`: which assembly built it, how it ranked, and whether that ranking
read vectors. Each producer states its own; the shape is identical either way, and a producer that
says nothing is reported as unknown rather than guessed at — silence about the ranking is not
evidence of either answer. `ranking_uses_vectors: false` survives compaction specifically because a
filter that drops falsey values would delete exactly the case worth reporting.

**Explore → Ask** shows *"Answered by …"* under the result count, and when the answering path did not
consult vectors it says so plainly: those results were ordered by how many of the query's words
appear in the text, so a paraphrase sharing no words will not match however well the encoder is
configured. That sentence is driven by what the backend reported, never by what is configured.

### Six checks that were one flag

The native pack also reported a `correctness_evidence` block — scope filtering, placement filtering,
the secondary-index prefilter, stale exclusion, a shared resource/skill quota, a cross-session quota
rerank — with **all six set from `selected_count > 0`**. One condition wearing six hats: a non-empty
pack marked every property verified, an empty one marked every property failed, and two of them are
not performed on that path at all.

Each field now comes from its own observation, and the counter that the six actually measured is
kept under its own name, `selected_any`. The derivation moved into a function taking one argument
per property, so a future edit wiring two fields to one input shows up in the signature rather than
four hundred lines into the caller. Five tests hold it: that each field follows its own input, that
flipping one input moves exactly one field, that a non-empty pack does not certify checks that never
ran, that an empty one does not fail checks that did, and that the two quotas this path does not
perform stay reported as not performed.

---

## The status strip

Every page carries a live strip in the nav: what is waiting to be encoded, the import that is
running, requests and failures, and how many configuration warnings are live. Each segment links to
the page that owns it, so it is a way in as well as a readout.

It exists because status used to be per-page. The stream already carried all of this, and two of the
seven pages subscribed — so *"is my upload done"* depended on being on the right page, and the page
you upload from was not one of them.

**One connection per page.** The gateway holds a task per stream, so a page opening a second to draw
one strip costs twice the server work for nothing on screen. A page that already runs its own stream
(Setup, Overview) claims it synchronously and hands frames to the strip; only pages without one
connect. The claim has to be synchronous — the page's script runs before the strip's, but its stream
starts when a fetch resolves, and claiming there would be too late for both to avoid connecting. A
hidden tab drops its connection and reconnects on return.

**Absent is not zero.** The frame omits `embedding` when the backend could not be asked, and the
strip says *encoding unknown* rather than *0 waiting* — the second is the reassuring answer to a
question nobody managed to ask. An empty store and an unreachable one both have nothing to report
about a backlog and mean opposite things, so they read differently. Nothing is drawn at all before
the first frame arrives.

**Read at the pace it is changing.** The encoding figures are refreshed every four seconds while
anything is waiting and every thirty when nothing is — the answer requires walking the record log,
so the slow case avoids re-deriving a constant for every connected browser, and the fast case is
what makes a draining backlog look like it is draining rather than stuck. Deferred work counts as
draining, and so does *unknown*.

**Anything can watch the same frames.** The strip takes subscribers, so a panel that needs live
state uses the connection the page already has rather than opening its own. Two do:

* a **batch started on Explore is watched on Explore** — it used to print a job id and a link to
  another page, which is a dead end at the moment a customer most wants to see something happen. A
  job missing from one frame is not treated as finished until it has actually been seen running,
  because a submission can be accepted before it appears;
* **Skills & resources follows the store** — ingest elsewhere, come back, and the list used to be
  whatever it was when the button was last pressed. It re-lists when the stored total changes, not
  on a clock, so a quiet deployment does no work and a busy one keeps up. It will not list before
  the customer has listed once (the page has no scope until then), will not re-list more than once
  every few seconds during an import, and does nothing in a hidden tab.

The strip's script is tested by running it: the page as shipped, through a small DOM stub, against
real frames. A strip whose source contains the word "unknown" and never reaches that branch greps
identically and is a different product. Four deliberate breakages — rendering unknown as zero,
drawing before the first frame, dropping the retry count, keeping a stale colour on a hidden
segment — each turn tests red.

Every page's scripts are also **executed** at load against a stub, which catches two things nothing
else here would. A stray brace produces a page that serves 200, renders its markup and does nothing,
with the error visible only in a browser console. And a subscription written as
`if (false) { …onFrame(…) }` satisfies any check that greps for the call: the first version of these
tests passed with the catalogue's subscription disabled exactly that way. The stub lists each browser
API a page reaches for rather than answering everything through a catch-all, because a catch-all
would also swallow a call into something that does not exist — which is the class of bug the harness
is for.

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

## Deployment

The storage directories, the metaserver address and the topology are **not** editable from the
settings form, and that is deliberate: repointing a running deployment's storage from a browser does
not reconfigure it, it strands its data. But "cannot be changed here" and "cannot be chosen at all"
are different things, and only the first was ever true. Choosing happens when a deployment is
*launched*, so that is where the chooser lives.

| Shape | What it is | Storage |
| --- | --- | --- |
| One box | Everything in a single process, no metaserver, no peers. The default, and it needs no configuration to work. | Local disk: EBS, instance SSD, or whatever the machine has |
| Replicated (Raft) | An odd number of nodes replicating through Raft, each with its own disk. | Local disk, plus a separate Raft write-ahead log directory |
| Shared storage | Nodes are stateless in front of one shared store. | MatrixObject by default, or a shared filesystem path |

`GET /v1/admin/deployment` lists the shapes and reports the backend this deployment is **actually
running**, read from the datanode's own `temporalstore_storage_backend` metric rather than derived
from configuration. `POST /v1/admin/deployment/plan` composes a choice and returns the environment,
an env file, what is blocked, and what will differ from what was asked. Neither route changes this
process.

### Why the plan reports a resolved backend and not just the requested one

Three choices produce a deployment that starts, serves, and is not the one that was selected — with
no error raised anywhere:

* `TS_STORAGE_BACKEND=shared` with no `TS_SHARED_STORE_DIR` **falls through to auto-detection**
  rather than failing. Ask for shared storage, get whatever auto picks.
* `TS_STORAGE_BACKEND=matrixobject` on a build without that feature compiled in takes the same
  fall-through, reached a different way.
* Standalone is **derived**, not defaulted: `!(meta_addr_is_real || TS_DISTRIBUTED)`. A metaserver
  address arriving from a config file or a systemd drop-in is enough on its own to turn a one-box
  deployment into a distributed one. A one-box plan therefore pins `TS_STANDALONE=1` rather than
  leaving it to the default, so that cannot happen quietly.

The engine records **why** it chose a backend in a startup log line and nowhere else, so the portal
reports the outcome and says plainly that the reason is not readable over HTTP.

The chooser also never asserts that MatrixObject is unavailable. A live backend of `matrixobject`
proves the feature is compiled in; anything else is silence, because auto reaches other backends for
several reasons on a build that has it. Turning a missing datanode into a claim about the build
would print a false statement next to a storage choice.

### Keys

Key **names** are part of a plan; key **values** never are. The plan document can be shown, copied,
exported and diffed without carrying a credential through any of those. Values are entered in the
settings form above, which stores them owner-only and never returns them from any read.

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

  # The engine's own Prometheus output, on the service port of each process. This is a different
  # surface from /v1/metrics and carries none of the same series.
  - job_name: matrixark_engine
    metrics_path: /metrics
    static_configs:
      - targets: ['proxy:17000', 'metaserver:17001', 'datanode:17002']
```

**Two targets, not one.** The gateway publishes at `/v1/metrics`; the data node, the metaserver and
each raft node publish at `/metrics` on the same listener they serve their service routes on. The
engine dashboard queries only the second set, so importing it against the gateway produces twelve
blank panels — and a blank panel reads as an idle cluster rather than as a query aimed at the wrong
process. The setup page prints this config with the engine host filled in from the datanode this
deployment is configured to dial, so what you copy resolves.

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
`GET /v1/admin/monitoring/{gateway|ingestion|engine|alerts|engine-alerts}`, and the setup page
lists them in a table with what each covers and which target feeds it.
The portal used to name a repo path, which a customer running this as a managed service cannot
reach — and even with a checkout, the file on their disk is whatever their copy is rather than what
this build emits, which is exactly the drift the dashboard test exists to prevent.


* `docs/ops/matrixark-gateway-dashboard.json` — config health, request rate, errors by status,
  p50/p95/p99 latency, byte throughput, ranked routes, plus when the configuration last changed,
  how many settings are away from stock, and worker uptime.
* `docs/ops/matrixark-ingestion-dashboard.json` — bulk import progress and backlog.
* `docs/ops/temporalstore-dashboard.json` — everything that is not the edge: Raft commit, apply and
  lease; the metaserver scheduler and topology; proxy routing, admission and quarantine; object and
  page store lifecycle; cache pressure; data node lifecycle; ingestion lag and dead letters; replica
  replay; and the scale SLO. Reads from the engine job, not the gateway job.
* `tools/temporalstore-prometheus/matrixark-gateway-alerts.yml` — gateway alert rules.
* `docs/ops/temporalstore-alerts.yml` — engine alert rules: Raft majority loss and stalled applies,
  scheduler backlog, proxy quarantine, cache miss pressure, replay failures and dead letters.

The engine dashboard and its rules were in the tree from the start and served by nothing, so a
customer setting up monitoring from the portal got the gateway and the importer and had no way to
find out the rest was monitorable at all. `test_matrixark_dashboards` now asserts the other
direction too — every monitoring asset in `docs/ops` is offered — because the forward check, that
every named asset resolves, passes perfectly while an asset is missing from the registry.

The shipped Prometheus stack (`tools/temporalstore-prometheus/`) loads the engine rules and scrapes
the engine job that feeds them. A rule file with no job behind it is not an error; it is permanent
silence, which reads as a healthy cluster.

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
