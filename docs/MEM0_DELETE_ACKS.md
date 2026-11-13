# Memory Delete Acknowledgments

The batch-delete endpoint counts a member as deleted only when its individual
response has HTTP status 200, `deleted: true`, and the requested memory ID.
HTTP success alone is not a deletion acknowledgment.

A concurrent delete can win after the record is read. If the resulting individual
response says `deleted: false`, the batch reports a per-member conflict with
status 409 and code `memory_delete_not_applied`, not an additional deletion.
Missing, malformed or mismatched successful responses report status 502 with
`invalid_memory_delete_response`. Non-success HTTP responses remain failures;
their structured error code is retained, including `write_outcome_unknown`.

The mixed batch response shape is unchanged: acknowledged members appear in
`results`, other members in `failed`, and `deleted` counts acknowledged members.
This validation does not retry a deletion or reinterpret an unknown outcome as
success, rollback or proof of data loss.

The change does not make the serial deletion workflow transactional or fix its
catalog/cleanup split. Those lifecycle and performance changes require separate
concurrency and recovery work. No timeout, WAL or retry setting changes.

```bash
cargo test -p temporalstore-rust --lib batch_delete_ack -- --test-threads=1
```
