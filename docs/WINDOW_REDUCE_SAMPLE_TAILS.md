# Window Reducer: sample tails

Design note. Nothing here is built yet; it fixes the shape before the code so the reader, the writer
and the retention changes agree on one record.

## The cost this removes

A merge write into a sample reads the whole stored sample, merges the request's cells into it,
encodes the whole result and stores it again. The bytes written and the CPU therefore grow with the
size of the sample, not with the number of cells the request carried. Measured through the engine,
one cell written into a sample of 64, 1,024 and 4,096 cells writes 897, 2,935 and 6,554 bytes
(write-ahead log, index log and block store together) and takes 2.3, 4.3 and 6.4 ms at the median.

## The idea

A merge write into a sample that already has a stored base is not rewritten. Its cells are appended
to that sample's **tail**: the ordered list of merge writes the base has not absorbed yet. The value
a reader sees is the **fold**: the base with every tail write merged into it, in order, by the same
`merge_into_sample` the write path uses today. Because the fold IS the write-time merge replayed,
every lane reducer, the label rule (the newest write's labels win), the created and merged counts and
the cell limit behave as they do now. A tail is a deferred merge, not a new semantic.

When a tail grows past its threshold the next write folds it: it writes the whole merged sample as a
new base, exactly as today, and clears the tail in the same log record.

## Where it lives

* **The tail is state of the key, beside the retry ring**: a new side-table domain `sample_tails`
  holds, per key, the tails of its samples by stamp. It is in the index image the way the ring is, so
  a restart and a follower catching up from an image get it.
* **Each tail write is one log item** of a new kind, `sample_tail`, staged in the same record as the
  command's other outcome items. Its value is protobuf: the stamp and the cells exactly as the request
  carried them (member, labels, lanes), spelled with the sample encoding the base uses. The record is
  atomic, so a torn tail of the log drops a whole write, never half of one.
* The series itself (stamp to block address) is not touched by a tail write: no block is written and
  no index entry is added. The index log gains no variant, so this change and the series-trim change
  do not share a record family.

## Reading

Every read of a stored cell sample goes through one function. Today the readers (merge planning,
ranked read, merged read, removal, rollup) each call the block read directly; they will call
`sample_value` instead, which returns the stored bytes untouched when the stamp has no tail and the
folded bytes (header, then the merged sample, encoded as a full sample is) when it has. The bytes are
what a whole-sample write would have stored, so no reader needs to know tails exist, and a
differential test can compare the two byte for byte.

Cost bound: only samples with a tail pay for a fold. A key holds tails for at most
`MAX_TAIL_STAMPS` stamps (a write to one more folds the oldest first); a tail holds at most
`max(64, base cells / 8)` cells and 256 writes. A read therefore folds at most `MAX_TAIL_STAMPS`
samples, each in one decode, merge and encode of that sample. A fold is remembered while the tail
does not change, so repeated reads of a quiet key pay nothing more.

## Writing

The write plans against the folded newest state as today (membership, created and merged counts,
the cell limit, the header check), then:

* base absent, tail absent: stored whole, as today;
* base present and the request's header equals the stored one and the tail has room: one tail write;
* otherwise (a header upgrade, an over-threshold tail, a request wider than the tail room): the
  merged sample is stored whole as the new base and the tail of that stamp is cleared.

Anything that replaces or removes a sample (rollup, removal, trim, expiry, key delete) reads it
through `sample_value` and clears that stamp's tail where it writes the replacement, in the same
record. That rule is the whole consistency story, and a model test drives every command in random
order against it.

## Recovery and replication

* Replay applies `sample_tail` items in log order: append to the stamp's tail. The clear that comes
  with a whole write, a rollup, a removal or a trim is applied in the same order, so replay ends where
  the leader was.
* Crash arms, each exercised by cutting the log at the record boundary and by tearing the last
  record: after a tail write; after a whole write that cleared a tail; between an image flush that
  holds a tail and the log written after it; a trim or removal after a tail write.
* A command replicates as it does now (the request), so the bytes on the wire do not change. Each
  replica writes the small tail item instead of a whole sample, so per-replica durable bytes fall.

## Expected size, to be measured

One cell into a large sample: about 100 to 200 bytes of log, whatever the sample size, plus the
share of the whole write that folds the tail (a tail of `base / 8` cells amortises to a few bytes
per write). The target is a flat figure in the sample's size; the measurement decides.

## Interplay with trimming

Trimming old samples (by count or by age) removes stamps. It reports the stamps it removed and the
engine drops those stamps' tails where it applies the trim, in the same record, during replay too.
