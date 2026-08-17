# Ingesting large skill / resource files from the caller side

When the file lives on **your** machine and the server is remote, don't stuff the raw
bytes into a JSON ingest body — that caps out and re-sends the whole file on every
retry. Instead, upload the file to the deployment's **blob tier** once and ingest a
tiny pointer (`temporalstore://<key>`). Uploads are **content-addressed and
idempotent**, so retries are safe and cheap (see [Retries](#retries)).

You have three ways to do it, from easiest to most explicit:

1. The one-function Python SDK helper `ingest_large_file(...)`.
2. The single-call HTTP endpoint `POST /v1/ingest_file` (stream the file body once).
3. The explicit two-step: `PUT /v1/blob/<key>` then `POST /v1/ingest`.

For small content, ingest text inline (no upload) — see [Small files / text](#small-files--text).

The only required argument is the file `path` (or `text`). The gateway `base_url` and
`api_key` are optional and resolved from the environment (see
[Zero config](#zero-config-base_url--api_key) below).

---

## 1. Python SDK helper (one function)

Zero config — only the path is required:

```python
from matrixark_ingest_client import ingest_large_file

# base_url and api_key come from the environment (see "Zero config" below)
result = ingest_large_file("/path/to/skill.md")
print(result)               # the ingest response JSON
```

Or pass everything explicitly:

```python
result = ingest_large_file(
    "/path/to/skill.md",
    base_url="https://api.example.com",  # optional; defaults to $MATRIXARK_BASE_URL / localhost
    api_key="YOUR_API_KEY",             # optional; defaults to $MATRIXARK_API_KEY, else anonymous
    kind="skill",              # or "resource"
    resource_type=None,        # inferred from the file suffix when omitted
    scope=None,                # optional; tenant is pinned by your API key
    sharing_scope="tenant_shared",  # visibility; omit for the server default
    wait=False,                # True -> wait for extraction; False -> fast 202 ack
)
```

It reads the file locally, content-addresses it, streams it to the blob tier once,
then ingests the `temporalstore://<key>` pointer. Standard library only — no extra
dependencies.

### Zero config (`base_url` / `api_key`)

Both `ingest_large_file(...)` and `ingest_text(...)` resolve the gateway and key from
the environment when the arguments are omitted:

| Argument | Resolution order | Default |
| --- | --- | --- |
| `base_url` | arg → `MATRIXARK_BASE_URL` → `MATRIXARK_GATEWAY_URL` | `http://127.0.0.1:8080` |
| `api_key` | arg → `MATRIXARK_API_KEY` | none → **no `Authorization` header** (anonymous) |

```bash
export MATRIXARK_BASE_URL="https://api.example.com"
export MATRIXARK_API_KEY="YOUR_API_KEY"   # omit for anonymous (dev gateway)
```

When no key is set, the client sends **no `Authorization` header** — which works
against a dev-mode gateway (the default posture, see [Authentication](#authentication)).
Against an enforced gateway an anonymous call returns `401`, surfaced by the client as
a clear `RuntimeError` (it does not crash). Explicitly passed `base_url` / `api_key`
always win over the environment.

**Retry rule:** on *any* error, just call `ingest_large_file(...)` again with the same
arguments. It is idempotent (same bytes → same key → dedup), so retries never create
duplicates and cost almost nothing.

---

## 2. Single-call HTTP endpoint: `POST /v1/ingest_file`

Stream the file body in **one** request; the server stores it to the blob tier and
ingests it for you.

| Header | Meaning | Default |
| --- | --- | --- |
| `Authorization: Bearer <key>` | Auth (required only when the gateway enforces auth) | — |
| `X-Resource-Kind` | `skill` or `resource` | `skill` |
| `X-Resource-Type` | `md` / `pdf` / `txt` / … | inferred from `X-Filename`, else `md` |
| `X-Filename` | original filename (used to infer the type) | optional |
| `X-Sharing-Scope` | visibility: `private_user` / `tenant_shared` / `global_shared` (alias `X-Visibility`) | server default |
| `X-Wait` | `true` to wait for extraction | fast-ack `202` |
| body | raw file bytes (streamed) | — |

`X-Sharing-Scope` (alias `X-Visibility`) is forwarded as the top-level `sharing_scope`
on the ingest — the same knob the SDK sends. An invalid value returns `400`; omitting it
applies the server default.

```bash
curl -X POST "$BASE_URL/v1/ingest_file" \
  -H "Authorization: Bearer $API_KEY" \
  -H "X-Resource-Kind: skill" \
  -H "X-Resource-Type: md" \
  -H "X-Filename: fix-auth.md" \
  -H "X-Sharing-Scope: tenant_shared" \
  -H "Content-Type: text/markdown" \
  --data-binary @skill.md
```

Response: `202` with the ingest result JSON (or a synchronous result when
`X-Wait: true`). The server computes the content-addressed key itself and spools the
body to disk while hashing, so memory stays flat no matter how large the file is.
Oversized bodies get `413`.

---

## 3. Explicit two-step (blob then ingest)

Useful when you want to upload once and ingest the same blob into several scopes, or
to control the key yourself. The key is `resources/<sha2>/<sha256>` where `<sha256>`
is the SHA-256 of the file bytes (`<sha2>` is its first two hex chars).

**Step 1 — upload the blob:**

```bash
# compute the content key
SHA=$(sha256sum skill.md | cut -d' ' -f1)
KEY="resources/${SHA:0:2}/${SHA}"

curl -X PUT "$BASE_URL/v1/blob/$KEY" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: text/markdown" \
  --data-binary @skill.md
```

**Step 2 — ingest the pointer:**

```bash
curl -X POST "$BASE_URL/v1/ingest" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"kind\":\"skill\",\"raw_uri\":\"temporalstore://$KEY\",\"resource_type\":\"md\"}"
```

---

## Small files / text

For short skills or resources, skip the upload and send the content inline:

```python
from matrixark_ingest_client import ingest_text

ingest_text(
    "Short skill body in markdown.",
    base_url="https://api.example.com",
    api_key="YOUR_API_KEY",
    kind="skill",
    resource_type="md",
)
```

or over HTTP:

```bash
curl -X POST "$BASE_URL/v1/ingest" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"kind":"skill","text":"Short skill body.","resource_type":"md"}'
```

---

## Sharing scope (visibility)

Both `ingest_large_file(...)` and `ingest_text(...)` take an optional `sharing_scope`
(alias `visibility`) that controls who can see the ingested skill/resource. It is sent
as the top-level `sharing_scope` key that the server already consumes; omit it to keep
the server's default (derived from your scope/deployment).

| `sharing_scope` | Visible to | Typical use |
| --- | --- | --- |
| `private_user` | only the ingesting user/session | personal notes, drafts |
| `tenant_shared` | everyone in your tenant/account | team skills & knowledge |
| `global_shared` | everyone on the deployment | org-wide / public content |

```python
from matrixark_ingest_client import ingest_large_file, ingest_text

# a team-wide skill file
ingest_large_file("/path/to/skill.md", base_url=BASE_URL, api_key=API_KEY,
                  kind="skill", sharing_scope="tenant_shared")

# short inline content, shared deployment-wide (visibility= is an accepted alias)
ingest_text("Short knowledge body.", base_url=BASE_URL, api_key=API_KEY,
            kind="resource", visibility="global_shared")
```

An unknown value raises `ValueError` locally before any request is sent.

---

## Size limits

Inline/skill content is size-capped so a runaway body can't be ingested by accident.
**Skills and resources share the same inline/text limit** (5 MiB) as a single source of
truth, so a skill and a resource of the **same size behave identically**:

| Content | Default cap | Env override |
| --- | --- | --- |
| `kind="skill"` (file bytes / inline chars) | **5 MiB** (shared) | `MATRIXARK_MAX_SKILL_BYTES` (legacy alias: `MATRIXARK_SKILL_MAX_TEXT_CHARS`); falls back to the shared resource cap |
| `kind="resource"` inline text | **5 MiB** (shared) | `MATRIXARK_RESOURCE_MAX_INLINE_TEXT_CHARS` |
| `kind="resource"` file | **20 MiB** | `MATRIXARK_RESOURCE_MAX_FILE_BYTES` |

The skill cap now **defaults to the shared resource inline/text limit** (5 MiB) rather
than a separate 2 MiB cap. A skill-specific override (`MATRIXARK_MAX_SKILL_BYTES` or the
per-request `max_text_chars=` argument) still wins when set. An oversized skill fails
loudly with the effective limit in the message, e.g.
`skill text too large: … chars, max is 5242880`. (A separate structural limit on the
number of parsed chunks, `MATRIXARK_RESOURCE_MAX_TOTAL_CHUNKS`, applies equally to both.)

---

## Authentication

**The gateway ships in a dev / no-auth posture by default** — the API works anonymously
out of the box with zero config, and the server logs a one-time startup warning that
auth is off. This is convenient for local development but **not safe for production**:
anyone who can reach the address has full anonymous access and there is **no tenant
isolation**.

**For production, opt in to authentication:**

```bash
export MATRIXARK_REQUIRE_AUTH=1        # reject requests without a valid API key (401)
export MATRIXARK_ACCESS_MODE=enforced  # enforced per-tenant hashed keys + isolation
```

When enforced, a missing or bad key returns `401`, and the tenant/account identity is
pinned from the key (never from client-supplied text), isolating tenants from each
other. The SDK sends its `Authorization: Bearer` header only when an `api_key` is set
(arg or `$MATRIXARK_API_KEY`); against an enforced gateway an anonymous call gets `401`.

---

## Retries

Uploads are **content-addressed and idempotent**:

- The blob key is derived from a SHA-256 of the file bytes, so the *same* bytes always
  produce the *same* key. Re-uploading is a de-duplicated no-op — never a duplicate.
- Therefore, on **any** error (network blip, timeout, 5xx, a partial upload), just
  **retry the same call**. There is no retry cap to manage, and retries are cheap.
- The server **integrity-checks the blob on fetch**: it verifies the stored bytes
  still hash to the key. A corrupt or partial upload fails **loudly** at ingest time
  (`sha256 mismatch (corrupt/partial upload) — re-upload and retry`) instead of
  silently ingesting bad data — which is your cue to retry.
