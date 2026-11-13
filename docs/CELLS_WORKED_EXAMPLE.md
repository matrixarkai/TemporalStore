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

Bounds: 8 dimensions, 4 labels, 16 lanes, 1 000 top-k, 512 snapshots per window, 8 predicates and
8 lanes in a weighted score.

## A worked example

`crates/temporalstore-rust/src/engine/tests/a_worked_example_of_cells.rs` is a runnable test that
doubles as this page's example. One user, two dimensions (`channel`, `kind`), a label (`slot`) and
two lanes (`impressions`, `clicks`, both summed): it writes four snapshots, merges two writes at
one timestamp, then ranks the last minute by click rate and by the Wilson lower bound. Item 1,
clicked once in one impression, leads by click rate and comes last by the bound; item 2, with 80
clicks in 100 impressions, leads by the bound.

## Not included

Rollup and shrink of old snapshots, and a time-to-live per group: a key has one time-to-live, as
every feature key does.
