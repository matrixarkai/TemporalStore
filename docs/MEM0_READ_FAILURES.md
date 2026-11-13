# Memory Read Failures

Addressable memory reads and the read stage of update, delete, and feedback distinguish
missing records from backend failures. A successful catalog lookup with no record, or a
tombstone, returns HTTP 404. A failed backend lookup returns HTTP 502 with its status code
and message. Incomplete responses, corrupt records, and mismatched record identities also
fail instead of becoming a missing memory. No model call or mutation follows a failed read.

History reads reject backend failures, invalid response envelopes, and undecodable entries
instead of returning an empty or truncated history.

Batch updates retain their HTTP 200 result envelope. Failed read cohorts have no successful
updates; their `failed` entries include `status`, `code`, and `body`. For example:

```json
{
  "results": [],
  "updated": 0,
  "failed": [{
    "memory_id": "memory-id",
    "status": 502,
    "code": "catalog_read_unavailable",
    "body": "catalog lookup could not complete"
  }]
}
```

The checked path uses the same catalog and tombstone multi-reads as before. It adds no
backend round trip and no automatic retry. A mutation's unknown outcome must not be
replayed merely because a client saw an error.

The Rust scale driver reports the first failed batch status and a bounded error-code
identifier. It does not copy memory contents or backend error bodies into the report.
This behavior improves error classification; it does not repair the underlying storage
failure or establish large-corpus performance acceptance. List and index-maintenance
fallback paths are separate from these addressable-operation guarantees.
