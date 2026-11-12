# Bucket Update Scratch Work

A block rewrite checks whether each affected routing bucket still holds another
live block of the object. Previously it materialized the routing bucket of every
surviving block before answering those membership questions.

The update now collects only affected buckets, sorts and deduplicates that small
list, and checks the existing borrowed survivor list with an early-exit search.
Scratch space is proportional to affected blocks, not all surviving blocks.
Worst-case scan work can still grow with survivors and distinct affected buckets;
this is not a constant-time lookup or a new resident index.

Routing identities, missing-lookup handling, block removal and object-index
retirement are unchanged. Buckets under an older routing range are checked by
their actual identity rather than assuming the current range contains them.

Four checks cover membership equivalence over 65,536 small sequence pairs,
duplicate and absent buckets, routing identity and bounded repeated-bucket work.
For two affected rows in one bucket, the fixture inspects two affected rows plus
one survivor at widths 1, 64, 4,096 and 16,384. A native allocation probe records
8 scratch bytes at 65,536 survivors instead of the previous 262,144 bytes.
Those figures describe the membership helper, not total write allocation or
measured HTTP latency. No timeout, retry or durability setting changes.

```bash
cargo test -p temporalstore-rust --lib affected_bucket_survivors -- --test-threads=1
```
