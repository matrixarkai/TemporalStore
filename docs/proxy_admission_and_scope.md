# Proxy admission control and account scope

The proxy in front of TemporalStore is the process that every client request
passes through before it reaches a data node. That position makes it the right
place to answer three operational questions, and this page describes how each is
configured.

1. **Whose data is this proxy allowed to serve?** — account scope
2. **How much concurrent work will it accept?** — in-flight quotas
3. **Which replica do reads come from?** — primary pinning

All three are proxy options: they can be set at startup, read back from
`GET /proxy/policy`, and changed on a running proxy with `POST /proxy/config`.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `ingestion_account` | `""` | Account (namespace) this proxy is scoped to. |
| `enforce_ingestion_account` | `false` | Reject requests whose namespace differs from `ingestion_account`. |
| `max_inflight_requests` | `0` | Concurrent request ceiling. `0` is unlimited. |
| `max_inflight_write_requests` | `0` | Concurrent *write* ceiling, on top of the total. `0` is unlimited. |
| `pin_primary_reads` | `true` | Default replica-read routing for tables opened through this proxy. |

Defaults are chosen so an existing deployment behaves exactly as before an
upgrade: enforcement off, quotas unlimited, reads pinned to the primary (which
already matched the client-side table default).

The proxy binary reads each of them from the environment at startup:

| Environment variable | Option |
| --- | --- |
| `TS_PROXY_INGESTION_ACCOUNT` | `ingestion_account` |
| `TS_PROXY_ENFORCE_INGESTION_ACCOUNT` | `enforce_ingestion_account` |
| `TS_PROXY_MAX_INFLIGHT_REQUESTS` | `max_inflight_requests` |
| `TS_PROXY_MAX_INFLIGHT_WRITE_REQUESTS` | `max_inflight_write_requests` |
| `TS_PROXY_PIN_PRIMARY_READS` | `pin_primary_reads` |

## Account scope

With `enforce_ingestion_account` set, any request carrying a namespace that is
not `ingestion_account` is rejected with `proxy_account_denied` before the proxy
opens a table or touches a data node. This covers the namespace-carrying paths:
`/proxy/open_table`, `/proxy/table_execute`, `/proxy/table_batch_execute`, and
every `/ProxyService/*` command that routes through them.

Enforcement with an empty `ingestion_account` is a misconfiguration, and it
**fails closed** — the proxy returns `proxy_account_not_configured` rather than
serving everything. A proxy told to enforce something must never be more
permissive than one that was not.

The shard-routed `/execute` and `/batch_execute` paths carry no namespace, so
there is nothing to scope; they are still subject to the quotas below. Deploy a
scoped proxy with those paths closed off at the ingress if the account boundary
has to be absolute.

## In-flight quotas

Each admitted request holds one slot in a total counter, and writes
additionally hold a slot in a write counter. Exceeding either returns
`proxy_inflight_quota_exceeded` / `proxy_write_inflight_quota_exceeded` — a
fast, cheap rejection at the front door rather than a queue that grows until a
timeout fires deeper in the stack.

The write quota exists because writes are the expensive side of the workload:
they take the durability path on the data node, so a write burst that consumed
every slot would stall reads that cost almost nothing to serve. Sizing
`max_inflight_write_requests` below `max_inflight_requests` reserves headroom
for reads under exactly that burst.

Slots are released by a guard tied to the request, so every return path —
success, backend error, or panic-free early return — decrements exactly once.

## Primary pinning

`pin_primary_reads` is the proxy-wide default for tables opened through it.
`true` routes reads to the primary, which is what read-after-write consistency
requires. Set it to `false` to allow follower or locality reads where a stale
read is acceptable and the latency saving matters.

An explicit `pin_primary` on an individual `/proxy/open_table` request still
wins over the proxy default, so a single latency-sensitive caller can opt out
without changing the policy for everyone.

## The high-level `/context/*` routes

`/context/ingest`, `/context/extract` and `/context/retrieve` are the routes the
context gateway uses, and they forward straight to the owning data node instead
of going through command execution. That shortcut is what keeps a fast-ack
ingest down to a single write, but it also meant those routes were the one class
of traffic that ignored proxy policy entirely: a proxy put into `not_serving` to
drain still served gateway ingests.

They now pass the same admission as command traffic — account scope, drain and
write-disable, drop percentage, and the in-flight quotas — before any forwarding
happens. Ingest and extract count as writes; retrieve counts as a read, so a
read-only proxy keeps serving retrieval while it stops accepting new context.

Account scope on these routes matches `scope.account_id`. A request with no
`account_id` is rejected while enforcement is on, for the same fail-closed
reason an unset `ingestion_account` is.

Because these routes return a transport status rather than embedding one in a
body, rejections carry an HTTP code the gateway can act on without parsing:

| Rejection | HTTP |
| --- | --- |
| `proxy_account_denied` | 403 |
| `proxy_inflight_quota_exceeded`, `proxy_write_inflight_quota_exceeded`, `proxy_traffic_dropped` | 429 |
| `proxy_not_serving`, `proxy_write_disabled`, `proxy_account_not_configured` | 503 |

The drop percentage keys on `account_id|tenant_id`, so shedding load removes
whole tenants rather than a random slice of every tenant's session — a partially
dropped session is worse than a dropped one.

## Observability

`GET /proxy/policy` reports the configured values, the live in-flight counts,
and the rejection counters. `GET /metrics` exposes the same as Prometheus
families:

| Family | Labels | Meaning |
| --- | --- | --- |
| `temporalstore_proxy_requests_total` | `kind="account_rejection"`, `kind="inflight_rejection"` | Rejections by cause. `kind="admission_rejection"` remains the total across all causes. |
| `temporalstore_proxy_inflight_requests` | `kind="total"`, `kind="write"` | Requests in flight right now. |
| `temporalstore_proxy_inflight_limit` | `kind="total"`, `kind="write"` | Configured ceilings; `0` is unlimited. |
| `temporalstore_proxy_account_enforcement` | — | `1` when account scope is enforced. |
| `temporalstore_proxy_pin_primary_reads` | — | `1` when reads default to the primary. |

Any non-zero rejection counter also shows up as a `degraded_reasons` entry in
`GET /proxy/preflight`, so a proxy shedding load is visible from the readiness
surface without scraping metrics first.

## Changing the policy on a running proxy

`POST /proxy/config` accepts the options above and applies them without a
restart. They are part of the proxy config hash, so `config_version` changes
when any of them change — which is what lets a rollout confirm every proxy in a
fleet picked the new policy up.

That endpoint replaces the whole options document, so an omitted field normally
falls back to its default. For the five admission options that default is the
permissive one, and a config push shaped before they existed would otherwise
turn account enforcement off without anyone asking it to. Keys absent from the
body therefore keep their running value; only an explicit key changes them.
Everything else on the endpoint keeps its existing replace-the-document
behaviour, so read `GET /proxy/config` first if you are rewriting the rest.