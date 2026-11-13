# Bounded Memory Deletion

The memory-client batch-delete route groups valid, unique memory IDs by tenant.
Each group is processed in pages of at most 128 IDs. A page uses one checked
catalog/tombstone read, one catalog-removal batch and one derived-cleanup batch
for confirmed removals. Index preparation runs once per tenant. Single deletion
and batch deletion share the same cleanup-command builder.

A removal is confirmed only by a successful top-level response, the exact number
of child responses, a successful child status and integer result 1. A concurrent
no-op returns a failed member, not an acknowledged deletion. Duplicate IDs are
reported as conflicts and cannot decrement counters twice. Missing IDs are
reported as not found. Invalid or uncertain responses do not trigger cleanup.

The existing HashDelete command now returns Integer(1) for a removed field and
Integer(0) for a missing field, for both single and batched execution. The result
comes from the actual removal under the engine write lock, not from a preceding
read. This replaces its previous Empty success response without changing the
command or WAL encoding. Clients that require Empty must accept the integer
response before upgrading. Memory batch deletion fails closed against an older
server that still returns Empty; deploy the data service before the proxy.

Cleanup still writes deletion history and removes lookup/order metadata while
adjusting identity counts. A failed cleanup makes every affected member fail;
it never writes an old record back. No automatic retries or deadline changes
are introduced. The mutation batches use the existing storage and WAL APIs.

For 16 confirmed removals in one tenant, the route issues two mutation batches
instead of 32 individual mutation RPCs. This is a call-count reduction, not yet
an endpoint latency, memory or throughput result. Storage work for individual
commands remains, and larger cleanup batches can still encounter contention.

This is not an all-or-nothing transaction. Primary deletion and cleanup remain
separate phases. A failed or unknown phase may leave derived metadata needing
repair, and concurrent content updates are not fenced by this change. Result
ordering follows tenant groups rather than input order; memory IDs identify
each outcome. Model extraction and semantic retrieval are unchanged.

Seven focused tests cover actual single/batch storage removal counts, 16-item
and multi-page success, unrelated-record
preservation, exact remaining total count, history, duplicate/missing IDs,
uncertain primary acknowledgments, partial child failure, cleanup failure and
incomplete responses. Existing cleanup and acknowledgment regressions remain
required. The counted CI step must execute all seven focused tests.
