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

All you need is your deployment's `BASE_URL` and an API key (`Bearer` token).

---

## 1. Python SDK helper (one function)

```python
from matrixark_ingest_client import ingest_large_file

result = ingest_large_file(
    "/path/to/skill.md",
    base_url="https://api.example.com",
    api_key="YOUR_API_KEY",
    kind="skill",            # or "resource"
    resource_type=None,      # inferred from the file suffix when omitted
    scope=None,              # optional; tenant is pinned by your API key
    wait=False,              # True -> wait for extraction; False -> fast 202 ack
)
print(result)               # the ingest response JSON
```

It reads the file locally, content-addresses it, streams it to the blob tier once,
then ingests the `temporalstore://<key>` pointer. Standard library only — no extra
dependencies.

**Retry rule:** on *any* error, just call `ingest_large_file(...)` again with the same
arguments. It is idempotent (same bytes → same key → dedup), so retries never create
duplicates and cost almost nothing.

---

## 2. Single-call HTTP endpoint: `POST /v1/ingest_file`

Stream the file body in **one** request; the server stores it to the blob tier and
ingests it for you.

| Header | Meaning | Default |
| --- | --- | --- |
| `Authorization: Bearer <key>` | Auth (required) | — |
| `X-Resource-Kind` | `skill` or `resource` | `skill` |
| `X-Resource-Type` | `md` / `pdf` / `txt` / … | inferred from `X-Filename`, else `md` |
| `X-Filename` | original filename (used to infer the type) | optional |
| `X-Wait` | `true` to wait for extraction | fast-ack `202` |
| body | raw file bytes (streamed) | — |

```bash
curl -X POST "$BASE_URL/v1/ingest_file" \
  -H "Authorization: Bearer $API_KEY" \
  -H "X-Resource-Kind: skill" \
  -H "X-Resource-Type: md" \
  -H "X-Filename: fix-auth.md" \
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
