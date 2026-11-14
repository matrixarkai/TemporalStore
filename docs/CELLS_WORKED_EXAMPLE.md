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
  `Weighted`, `Decay`) and `top_k`. It reduces each member across the window's snapshots, scores it and
  returns the best `k`. A name the key does not declare is refused.
* **Rollup.** `CellRollup` takes `older_than_ms`, `bucket_ms` and an optional cap. Every snapshot
  stamped strictly below the cutoff is folded, with the others of its bucket, into one snapshot
  stored at the bucket start. Cells merge by member, oldest snapshot first. A `Count` lane holds
  counts, and counts add; `Sum` adds (and saturates), `Max` and `Min` compare, `First` keeps the
  oldest value and `Last` the newest. The cap keeps the best `keep` cells of each group (a label's
  value, or all cells), scored like a ranked read, ties by member, and runs after the fold. The
  receipt says snapshots before and after, cells before and after, and cells dropped. Rolling
  an already-rolled bucket again, with the same arguments, changes nothing.
  Optional fields extend it, and a request that sets none of them is the request above, unchanged:
  `coarser_tiers` add tiers (an `older_than_ms` and a `bucket_ms` each, finest first, a coarser
  bucket a multiple of the finer, a last bucket of `u64::MAX` being the catch-all, one snapshot at
  stamp 0), so recent history stays fine and old history gets coarser in one request; `anchor`
  reads every `older_than_ms` as an age against the newest snapshot the key holds
  (`newest_minus_age`) instead of a stamp; `truncate` keeps the newest `keep_last` snapshots, acting
  once the key holds more than its `trigger`; and the cap takes `protect_newest_ppm` (a share of
  `keep` reserved for the members seen most recently) and `trigger_over` (a group is cut only when
  it holds more than `keep + trigger_over`).

Bounds: 8 dimensions, 4 labels, 16 lanes, 1 000 top-k, 512 snapshots per window, 8 predicates and
8 lanes in a weighted score.
A rollup touches at most 1 024 snapshots below its first cutoff (a longer history is rolled in
steps, by moving the cutoff forward), keeps between 1 and 4 096 cells per group, adds at most 8
coarser tiers, truncates to between 1 and 1 000 000 snapshots (trigger at most 10 000 000), protects
at most 1 000 000 parts per million of `keep` and tolerates at most 4 096 members over it. A bucket
whose members would exceed the 4 096 cells of one snapshot is refused: use a smaller bucket. A
request the key cannot serve is refused before anything is written.

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

### Tiers, an age and a truncation

`a_tiered_request_coarsens_old_history_in_one_command_and_ranks_the_same`,
`an_age_resolves_against_the_newest_stored_snapshot_and_follows_it` and
`a_truncation_acts_over_its_trigger_and_keeps_the_newest_snapshots`, in the same test file, work one
twelve-snapshot key (stamps 100 to 1 200). Below 1 100 the buckets are 200 wide, below 700 they are
400 wide (700 is aligned down to 600, a multiple of 200), so the twelve become seven, at 0, 400,
600, 800, 1 000, 1 100 and 1 200. Asked as ages (100 and 500) against the newest stamp, 1 200, the
same request gives the same bytes; after a snapshot at 1 300 arrives the same ages are other
cutoffs (1 200 and 800) and the key settles at six snapshots. With a `truncate` of the newest 4,
the seven become 800, 1 000, 1 100 and 1 200.

## Retrying a merge append

A merge-reduce append counts its `Sum` and `Count` lanes again when it is sent twice, so a client
that does not know whether a request landed cannot simply send it again. A request may carry a
**retry token**, a `u64` the caller chooses (`retry_token` on the append; zero is a token like any
other). The first request with a token is applied as usual. The same token again, on the same key,
answers the receipt the first application landed, with `replayed` set, and writes nothing to the
store.

* **Per key, and a window of 16.** A key remembers the last 16 tokens it applied, each with its
  receipt. The 17th evicts the oldest. A retry that arrives after 16 newer tokens have been applied
  is not protected: it is applied again as a new request. The window is a count of appends, never a
  time, so it is the same on a leader, on a follower and on a replay of the log.
* **Only a request that was applied files its token.** A request that is refused (a header that
  disagrees with the stored one, a bound) leaves its token unused, so the corrected request goes
  through. A replay is admitted before the request is checked at all: a retry of a request that was
  accepted is never refused for what the key has grown into since.
* **Only merge-reduce.** A token on any other write policy is refused, not ignored.
* **A key that goes forgets its tokens.** Deleting the key, its expiry, and a trim that leaves it no
  snapshot each clear the ring: the same token on the key written again is a new request.
* **Durable.** The rings are stored with the shard's index image (a new `retry_rings` domain, one
  byte string per key), so they come back after a restart and after the log has been reclaimed.
  Like a deadline or a seen set, a ring is held in the image and not in the blocks: tokens filed
  since the last image flush are rebuilt where the log is replayed (a replicated deployment), and
  a single node that crashes without replaying a log forgets them. A shard that never used a token
  stores exactly the bytes it stored before, the image stamp aside, and a key that never uses one
  pays nothing.
* **What the answer says.** `snapshots_written`, `cells_created` and `cells_merged` are the first
  application's, so a retry of a request that created a cell reads "created", where a request that
  merged again would read "merged". A request without a token is applied every time, as before.

A replay still reaches the log as a small record that changes nothing when applied.


## Narrowing a ranked read: members, anchor, quota, decay

A `CellRankQuery` can carry four more things. A read that carries none of them is encoded and
answered exactly as before: the fields are left out of the encoding when they are at rest, and a
spec that lacks them reads as one that has them at rest.

* **Member set** (`members`). `include` is a set the member must belong to and `exclude` one it must
  not; both apply to a cell before the member's snapshots are reduced, together with the predicates.
  `include` left out restricts nothing, and an include set that names nothing is refused (an empty
  list of members to read is almost always a caller that lost its list). A member in both sets is
  excluded. Each set holds at most 1 024 entries as sent, duplicates included, and a bytes member at
  most 256 bytes; a request past either is refused at admission. The request's order and repeats do
  not change the answer.
* **Anchor** (`anchor`: `request`, the default, or `newest`). A `Last` window ends at the clock the
  request carries; with `newest` it ends at the newest snapshot the key holds, so a key whose writes
  stopped reads the same whenever it is read. With newest stamp `n` and duration `d` the window is
  `[n + 1 - d, n]`, floored at zero: it spans exactly `d` stamps, and `d = 0` holds nothing. (On the
  request clock the window is `[now - d, now]`, one stamp wider.) An anchor on `Between`, `Since` or
  `LastN` is refused: they have no clock to replace.
* **Quota** (`quota`: `lanes`, each a lane name and a `count`, and `others`). The ranking is walked
  best first over the `top_k` members. A member that has a non-zero reduced value in a lane still
  short is kept and counts toward every short lane it fits; a member that fits none is kept while
  fewer than `others` such members were kept; the walk ends when every lane is full. It only drops
  members from the top-k list, so it never returns more than `top_k` rows, and a lane no member can
  fill leaves the answer short rather than padded. Counts plus `others` are at most 1 000, a quota
  names at most 16 lanes, a count is at least 1 and a lane is named once.
* **Decay score** (`CellScore::Decay { lane, half_life_ms }`). A member's score is
  `sum(value * 0.5^(age / half_life_ms))` over the values its snapshots store for the lane, with
  `age` measured from the newest snapshot of the window, so no clock is read. The half-life is
  between 1 ms and ten years. The total is a float and cannot wrap. A rollup cap refuses it: a cap
  scores one folded cell.

### An example

`a_quota_selects_exactly_as_a_fold_of_the_ranking_says`, in
`crates/temporalstore-rust/src/engine/tests/a_ranked_cell_read_is_a_command.rs`, stores one snapshot
of eight members with the lanes `shown` and `clicked`. Ranked by `shown` they come 1 to 8, and only
members 3, 6 and 8 have a click. The plain top four are 1, 2, 3, 4. With a quota of two members with
a click and `top_k` 8, the answer is members 3 and 6; with `top_k` 5 it is member 3 alone, because
the quota never reaches past the top-k; with `others` 2 added, members 1 and 2 come first, so the
answer is 1, 2, 3, 6.

`an_anchored_window_reads_the_newest_snapshot_whatever_the_clock_says` stores four snapshots at
100, 200, 300 and 400 and ranks by `shown`: an anchored `Last` of 100 is `[301, 400]`, one snapshot;
300 is `[101, 400]`, three; 301 reaches the snapshot at 100. The same `Last` of 100 on the request
clock, read long after the writes, is empty.

On the wire (JSON), a read that uses them looks like this; the three keys appear only when set:

```json
{"window":{"last":{"duration_ms":150}},"filters":[],"score":{"lane":{"lane":"shown"}},"top_k":5,
 "project":[],"members":{"include":[{"u64":1}],"exclude":[{"u64":2}]},"anchor":"newest",
 "quota":{"lanes":[{"lane":"peak","count":2}],"others":1}}
```

## Reading across keys

When the dimensions you always query by are in the key, a question that spans them ("which items did
this user respond to best over ALL channels?") spans keys. `CellFanInRankQuery` takes up to 64 keys
that share one spec and the same read as `CellRankQuery` (window, predicates, score, `top_k`,
projection). It merges the cells of every key BEFORE it scores, so the answer is the one a single
key holding all their snapshots would give. Merging the per-key top lists afterwards cannot match
it: an item that is second in each of two keys can lead their sum.

* The window applies to each key: `LastN { n }` is the newest `n` snapshots of every key.
* Cells combine in stamp order, a tie between keys broken by the key's position in the request, so
  `Sum`, `Max` and `Min` lanes do not depend on the order of the keys while a `First` lane takes the
  earliest stamp and a `Last` lane the latest.
* Keys whose headers differ only by labels or lanes added at the end read under the widest; any
  other difference is refused naming the key.
* A key the shard does not hold, an expired key, and a key with nothing in the window contribute
  nothing. `keys` in the answer counts the keys that did, so a caller who named five and got four
  can tell the answer is partial.
* The keys are read on the shard the request names (it is routed by the first key), so they must
  live together.
* Bounds, each refused whole and never truncated: 64 keys, 1 024 snapshots and 262 144 cells per
  request, a key named once.

### A batch of independent reads

When the questions are separate, `CellRankBatchQuery` carries up to 64 reads, each with its own key
and read spec, and answers them in request order, one `RankedCells` each. The reads do not affect
one another: a key with nothing to rank answers empty, an expired key is dropped and answers empty,
and a read the key cannot serve (an undeclared name, say) answers with `refused` set while the rest
are answered normally. Each answer equals the one `CellRankQuery` gives for that key and spec. A
read over a bound (top-k, window, predicates) refuses the whole batch at admission, naming the
read's position, before any key is read. As with the fan-in read, the keys are read on the shard the
request names.

`crates/temporalstore-rust/src/engine/tests/a_ranked_read_over_several_cell_keys.rs` is the runnable
example: one user, a `channel` dimension in the key, three channels and six snapshots, ranked by a
summed lane and by a maximum lane, checked equal to a single key that holds the same six snapshots,
and then read again after a restart.

## Not included

A time-to-live per group: a key has one time-to-live, as every feature key does.
