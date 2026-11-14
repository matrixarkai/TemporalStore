# Cells: merged, windowed, ranked statistics

A feature key can hold, at each timestamp, a snapshot of **cells**. A cell is a **member** (an id
or bytes), up to four named `u64` **labels**, and between one and sixteen named `i64` **lanes**.
Nothing in the engine knows what the names mean; a deployment declares them once.

* **Key.** An **entity** (a `u64`, text or bytes) and up to eight named **dimensions** become one
  stored feature key, so a read by those values is one lookup.
* **Header.** The first write of a key family carries the key spec, the label names and each lane's
  merge verb (`Sum`, `Max`, `Min`, `First`, `Last`, `Count`). Every later write is checked against
  it, and a mismatch is refused naming the differing field.
* **Write.** `FeatureAppendWithPolicy` with `MergeReduce` merges the request's cells into the
  snapshot already stored at that timestamp, lane by lane, and answers a receipt: snapshots
  written, cells created, cells merged.
* **Ranked read.** `CellRankQuery` takes a window (`Between`, `Last`, `Since`, `LastN`), named
  predicates over labels, lanes or `member`, a score (`Lane`, `Ratio`, `Wilson`, `Member`, `Time`,
  `Weighted`) and `top_k`. It reduces each member across the window's snapshots, scores it and
  returns the best `k`. A name the key does not declare is refused.
* **Rollup.** `CellRollup` takes `older_than_ms`, `bucket_ms` and an optional cap. Every snapshot
  stamped strictly below the cutoff is folded, with the others of its bucket, into one snapshot
  stored at the bucket start. Cells merge by member, oldest snapshot first. A `Count` lane holds
  counts, and counts add; `Sum` adds (and saturates), `Max` and `Min` compare, `First` keeps the
  oldest value and `Last` the newest. The cap keeps the best `keep` cells of each group (a label's
  value, or all cells), scored like a ranked read, ties by member, and runs after the fold. The
  receipt says snapshots before and after, cells before and after, and cells dropped. Rolling
  an already-rolled bucket again, with the same arguments, changes nothing.

Bounds: 8 dimensions, 4 labels, 16 lanes, 1 000 top-k, 512 snapshots per window, 8 predicates and
8 lanes in a weighted score.
A rollup touches at most 1 024 snapshots below its cutoff (a longer history is rolled in steps, by
moving the cutoff forward), keeps between 1 and 4 096 cells per group, and a bucket whose members
would exceed the 4 096 cells of one snapshot is refused: use a smaller bucket. A request the key
cannot serve is refused before anything is written.

## A worked example

`crates/temporalstore-rust/src/engine/tests/a_worked_example_of_cells.rs` is a runnable test that
doubles as this page's example. One user, two dimensions (`channel`, `kind`), a label (`slot`) and
two lanes (`impressions`, `clicks`, both summed): it writes four snapshots, merges two writes at
one timestamp, then ranks the last minute by click rate and by the Wilson lower bound. Item 1,
clicked once in one impression, leads by click rate and comes last by the bound; item 2, with 80
clicks in 100 impressions, leads by the bound.

## A worked example of a rollup

`a_worked_example_of_a_rollup`, in
`crates/temporalstore-rust/src/engine/tests/a_cell_rollup_is_a_command.rs`, follows one user
(entity 42) on one channel. Three minute snapshots hold items 1, 2 and 3 with the lanes
`impressions` and `clicks` (summed) and `peak` (max), and a fourth snapshot comes an hour later.
Rolled per hour with the cutoff at the hour, the three minutes become one snapshot at the start of
the hour: item 1 has 60 impressions, 3 clicks and a peak of 9, item 2 has 10, 5 and 2, item 3 has
100, 1 and 3, and the later snapshot is untouched. The receipt reads 4 snapshots before, 2 after,
6 cells before, 3 after. Capped to one item per `slot` by click rate (`clicks / impressions`),
item 2 (0.5) stays and item 1 (0.05) goes; item 3 is alone in its slot.

## Not included

A time-to-live per group: a key has one time-to-live, as every feature key does.
