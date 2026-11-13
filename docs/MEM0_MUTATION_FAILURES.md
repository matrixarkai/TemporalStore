# Memory Mutation Acknowledgments

Memory add, single update, batch update, delete, scope deletion, reset, and index preparation
validate the aggregate batch status, response count, and every child command status. An
aggregate success is not sufficient when a catalog, history, order, or membership write failed.

Addressable mutations return HTTP 502 for failed or incomplete acknowledgments. Batch update
retains its HTTP 200 envelope but places unsuccessful entries in `failed`, including the
backend `code`, and does not count their cohort as updated. An unknown write outcome remains
an error even when the write may have reached storage. Callers should reconcile the memory
and history before deciding how to proceed, not assume that an error means nothing applied.

Failed child acknowledgments do not trigger new retries or compensating writes. Existing
outer-failure compensation is unchanged. These checks are not transactional rollback and
do not make partially applied mutations atomic.

Catalog scans used for rebuilding indexes reject backend failures, malformed responses,
undecodable records, and identity mismatches. An unsuccessful scan or backfill cannot be
treated as an empty, complete index. Readiness is published only after confirmed backfill.

Reset first withdraws readiness markers, then confirms cleanup, then republishes readiness.
Failed cleanup does not publish new ready markers. This adds two administrative batch round
trips to scope reset, not to the normal add/update serving path. Reset remains multi-step;
callers must coordinate it with concurrent mutations.

These guarantees prevent false success reporting. They do not establish large-corpus
latency, throughput, or production acceptance, and do not repair the underlying storage I/O
or maintenance contention seen in longer workloads.
