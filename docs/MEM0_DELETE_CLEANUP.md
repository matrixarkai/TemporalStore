# Memory Deletion Cleanup

Memory deletion first acknowledges a catalog removal or a serving tombstone,
then updates derived lookup, ordering, history and identity state.

If cleanup fails, the endpoint reports the failure without compensating writes
that recreate the old catalog row or clear an accepted tombstone. This applies
both when cleanup was rejected and when its outcome is unknown. Such writes
could otherwise revive a memory after cleanup had actually committed, or race
with a later operation and restore stale data.

The successful response is unchanged. Failed batch members remain in `failed`,
not `results`; an unknown outcome is not reported as a successful deletion.
A failed request does not imply the earlier logical removal was rolled back.
Clients must reconcile state rather than assume an error means no change.

This is not a transactional deletion workflow or an automatic repair queue.
If cleanup did not apply, derived indexes and counters can still need repair.
The serial batch-delete implementation and performance limits remain separate
work. No deadline, retry, WAL or retention setting changes.

Tests cover single and batch responses, scoped tombstones, unrelated users,
applied-but-unknown cleanup, rejected cleanup, and checkpoint import into an
empty destination WAL after materializing source blocks. The checkpoint test
checks persisted logical state; it is not a network-fault or crash-replay test.

```bash
cargo test -p temporalstore-rust --lib delete_cleanup_failure -- --test-threads=1
```
