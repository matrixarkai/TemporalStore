# Window Reducer protocol

A wire protocol for adding, restoring, removing, deleting and reading merged cell samples
(`docs/WINDOW_REDUCE_WORKED_EXAMPLE.md`). The messages live in `proto/temporalstore/windowreduce/v1/windowreduce.proto`.
Field numbers and wire types are fixed so that clients speaking this message shape are understood;
every name is ours, and the behaviour is a generalisation: anything the narrower shape could say,
this one says the same way.

Naming: the model is the Window Reducer (WindowReduce). The unit it stores at one timestamp of a key, all the
cells of that key at that timestamp, is a **sample**. The command, message and field names in this page are the
WindowReduce names (`WindowReduceRank`, `WindowReduceAnswer`, `samples_written`, `sample_info`, and so on). The
rename changed no field number, no wire type and no stored byte; the stored key tag `cells` and the retry item
kind `cell_retry` keep their original spelling because they are written to disk.

## Conventions

* Numbers 1..15 of a message are the established ones. Numbers 16 and up are additive; a reader
  that does not know one skips it.
* Timestamps on the wire are microseconds; the store keeps milliseconds, so a timestamp is floored
  to a millisecond (accepted and documented). Two observations in one millisecond share a sample and merge.
* An entity is a 64 bit id (field 1), or text or bytes through the additive fields.
* Lane values are 64 bit. Readers that declare them 32 bit see the low 32 bits.
* A table name selects a declared **profile** (below). An unknown table answers `NO_SUCH_TABLE`.

## Profile

A deployment declares, once, per table name: the key spec (namespace, entity type, dimensions), which
dimension carries `channel` and which carries `kind` (an unbound one must be zero on the wire), the
label name that carries `label`, how a label is derived when a request leaves it out (`FromRequest`,
or `FromMember { shift, bits }`), the lanes with their reducers, the table shape (pair: two
lanes; list: up to 16), whether a duplicate member at one timestamp merges or is refused, and the
oldest and newest accepted timestamps. The profile is immutable for the life of the process; the
header written with the first write of a key family still guards every later write.

## Field numbers

| Message | Field | Number | Type |
|---|---|---|---|
| WindowReduceAddRequest | table, observations, group_on_server, skip_existing, retry_token | 1, 2, 3, 4, 16 | bytes, repeated message, bool, bool, uint64 |
| WindowReduceAddResponse | code, message, samples_written, cells_created, cells_merged, replayed | 1, 2, 16, 17, 18, 19 | enum, string, uint64 x3, bool |
| Observation | entity, timestamp_us, kind, channel, entries, entity_text, entity_bytes | 1, 2, 3, 4, 5, 16, 17 | int64, int64, int32, int32, repeated message, string, bytes (6 reserved) |
| RestoredObservation | entity, latest_us, earliest_us, kind, channel, entries, entity_text, entity_bytes | 1, 2, 3, 4, 5, 6, 16, 17 | as above (7 reserved) |
| CellEntry | member, shorthand_first, shorthand_second, shape, pair, list, label, has_label, member_bytes | 1..8, 16 | int64, int64, int64, enum, message, message, int32, bool, bytes |
| PairLanes | first, second, timestamp_us | 1, 2, 3 | int64 |
| LaneList | timestamp_us, values | 1, 2 | int64, repeated int64 |
| WindowReduceQuery | entity, window, selector, echo, table, has_echo, entity_text, entity_bytes, more_entities | 1, 2, 3, 4, 5, 6, 16, 17, 20 | int64, message, message, bool, bytes, bool, string, bytes, repeated int64 |
| WindowReduceBatchQuery | queries | 1 | repeated message |
| WindowReduceAnswer | entity, first_us, last_us, entries, sample_info, echoed_selector, echoed_window, code, message, samples, unscored, keys | 1, 3, 4, 6, 7, 8, 9, 16, 17, 18, 19, 20 | (2 and 5 unused) |
| WindowReduceBatchAnswer | answers, code, message | 1, 2, 3 | |
| WindowReduceSelector | see the proto (`members` 18, `quota_lanes` 14, `quota_counts` 15, `decay` 10, `decay_rounding` 11, `excluded_members` 40) | 1..30, 40 | presence flags are separate bools |
| Window | kind, span, start_us | 1, 2, 3 | enum, int64, int64 |

Enum values: operations ADD 0, BATCH_QUERY 1, REMOVE 2, RESTORE 3, DELETE 4. Status codes OK 0,
ERROR 1, STORAGE_ERROR 2, OUT_OF_MEMORY 3, NO_DATA 4, QUOTA_EXHAUSTED 5, REQUEST_ILLEGAL 6,
NO_SUCH_TABLE 7, INVALID_ARGUMENT 8, DUPLICATE_REFUSED 9. Rank modes FIRST_LANE 0, SECOND_LANE 1,
RATIO 2, MEMBER 3, RECENT 4, NAMED_LANE 5, MATCH_LANE 6, WEIGHTED 7, MEMBERS_ONLY 8, WILSON 9,
DECAYED_LANE 10 (new).
Window kinds TRAILING_FROM_NOW 0, TRAILING_FROM_NEWEST 1, NEWEST_ROWS 2, FROM_START 3,
TRAILING_FROM_NEWEST_OF_KIND 4 (the same as 1). Rounding FLOOR 0, CEIL 1, ROUND 2.

## Semantics

**Add.** Observations that share an entity, channel, kind and millisecond become one sample of
one key; cells with the same member merge lane by lane under the profile's reducers. The whole
request is validated before anything is written: a refusal writes nothing. A list shorter than the
profile's lanes is padded with zeros; a longer one is refused. The shorthand fields 2 and 3 fill a
pair when neither pair nor list is present. A cell with no `has_label` takes its label from the
profile's rule. `skip_existing` is carried and ignored: an add always merges (a retried add is made safe by the
retry token). `group_on_server` is accepted and always on.

**Retry token.** A non-zero `retry_token` (add request, field 16) rides on the append of every key of the
request; zero means none. A key remembers its last 16 tokens: the same token again answers the counts of
the first application with `replayed` (response field 19) set and writes nothing. `replayed` is true on
the response only when every key replayed. A token is not carried by a restore.

**Restore.** Writes pre-aggregated cells at `latest_us`; `earliest_us` must not exceed it and is
checked only.

**Batch query.** N independent reads, answered in request order, one answer each, each with its own
status. Bounded by the number of queries.

**Windows.** `TRAILING_FROM_NOW` is `[now - span, now]`. `TRAILING_FROM_NEWEST` is the trailing span
ending at the newest stored sample of the key. `FROM_START` is `[start, start + span)`; for the two
reads that select cells by member or by lane value (`MEMBERS_ONLY`, `MATCH_LANE`) it is
`[start, start + span]`, the stamp at the very end included (a span of 0 is the instant at the start).
`NEWEST_ROWS` counts stored (timestamp, member) rows, not samples: `span` is the number of rows, the
newest `span` cells the key holds, taken whole sample by whole sample from the newest back. The
sample that crosses `span` keeps only the cells it needs, its largest members (the newest rows in
(timestamp, member) order). A key that holds fewer rows is read whole. Rows are counted before the
label predicate and the member sets apply, as a sample is chosen before either does. At most 262,144
rows, and the rows may not reach back over more than 512 samples (refused, not cut short). The
window applies to each key of a merged read. It takes the request clock for no purpose, so it has
no anchor.

**Rank modes.** FIRST_LANE and SECOND_LANE order members by their reduced lane 0 or 1; NAMED_LANE by
the lane at `sort_lane` (zero when absent); RATIO by lane 0 over lane 1; MEMBER by member;
WEIGHTED by the sum of the lanes at `weighted_lanes` times `weights` (indexes and weights not
negative, same length, pair tables only index 0 and 1); WILSON by the lower bound for lane 0
successes of lane 1 trials at `wilson_z` (1.96 when absent). Results are ordered best first; equal
scores order by member. MEMBERS_ONLY returns the listed members and ignores `limit`. RECENT and
MATCH_LANE return stored cells newest first, not reduced per member (see **Stored cells** below).

**Member set.** `members` (with `has_members`) is the include set and `excluded_members` the exclude
set: a read uses only members in the first (when it is present) and never one in the second; a member
in both is excluded. A member list that holds anything counts as present even when `has_members` is
left off; a flagged, empty include set is refused (a caller that lost its list would otherwise read
nothing and not know it). An absent include set restricts nothing. Each side holds at most 1024
entries as sent, duplicates included. Members are 64 bit ids; the sets are applied to a cell before
that member's samples are reduced.

**Anchored windows.** `TRAILING_FROM_NEWEST` and `TRAILING_FROM_NEWEST_OF_KIND` are the trailing
`span` ending at the newest sample the key holds, so a key whose writes stopped reads the same
whenever it is read. The window is `[newest + 1 - span, newest]` in milliseconds: a span of S ms covers
exactly S stamps (`TRAILING_FROM_NOW` is one stamp wider). `NEWEST_ROWS` has no anchor.

**Quota.** `quota_lanes[i]` is a lane position (named through the profile) and `quota_counts[i]` how
many members in the answer must have a non-zero value in it; the answer never holds more than `limit`
rows, and a quota only chooses which of the top `limit` ranked members stay. `quota_counts[0]` is
also the count of members that fit no still-unmet lane (`others`). One count and no lane asks for
`others` alone. Counts are not negative, a lane is named once, there are at most 16 lanes, and the
reserved total (every lane count plus `others`) is within the top-k cap of 1000.

> **Known double count.** Because the first count is read both as `others` and as the first lane's
> count, a valid request whose counts add up to 500 reserves 1000 members, and one that adds up to 501
> is refused for reserving 1002. The first lane's count must also be at least 1. The mapping is kept
> as documented so existing senders read the same; it is pinned by
> `a_quota_whose_wire_counts_sum_to_500_reserves_1000_and_one_more_is_refused` and is for the owner of
> the wire shape to decide, not something this layer changes silently.

**Members only.** `MEMBERS_ONLY` answers exactly the members of `members` (at least one, at most
1000) and ignores `limit`: the read asks for as many rows as the set holds, one row per member that
holds a sample in the window, newest first (by the stamp of its newest sample, equal stamps by
member, smaller first). The selector's label is still an equality predicate, as for every read.

**Decayed lane.** `DECAYED_LANE` (a new enum value; the fields are the existing ones) ranks members by
the lane at `sort_lane` (the first when absent), each stored value halved for every `decay`
milliseconds of age measured from the newest sample of the window, and summed. `decay` is the
half-life; it is made a whole number of milliseconds by `decay_rounding` (floor, ceiling or nearest).
It must be finite, not negative, and, after rounding, between 1 ms and ten years. Every other mode
still accepts `decay` and ignores it.

**Stored cells.** `RECENT` and `MATCH_LANE` answer the cells as they were stored, one row per cell and
none reduced, the newest sample first (cells of one sample by member, smaller first), so a member
written in three samples is three rows, each with that sample's labels, lane values and stamp
(`timestamp_us`). `limit` bounds the rows (1 to 1000, as for every read); the window, the label
predicate and the member set apply to each cell. `MATCH_LANE` keeps the cells whose lane at
`match_lane` (the first when absent) holds `match_value` (0 when absent, any signed number). A lane the
table does not declare holds nothing, so no cell matches and the answer is empty. `NAMED_LANE` on a
list table with a `sort_lane` past the last lane scores every member 0 (members come back in member
order); a negative `sort_lane` is refused. A quota is refused on these modes: it chooses among reduced
members. Over `more_entities` the rows of all keys interleave by stamp, the later entity first when
two samples share one. The command is the ranked read with the score `Recent`.

**Several entities.** `more_entities` (at most 63, 64 bit ids, none repeated, none equal to `entity`)
turns the query into one merged read over the keys of `entity` and each of them, same channel and kind:
the answer one key holding all of their samples would give, not a merge of per-key top lists. The
window applies to each key. `WindowReduceAnswer.keys` says how many of the keys held a sample in the window.
The keys must live on one shard. A table whose entity is not a 64 bit id refuses it.

**Batch.** The queries become one batch command of independent reads, at most 64, answered in request
order, one answer each. A query with `more_entities` has no place in a batch and is refused, naming its
position; so is any query that would be refused alone.

**Answers.** `first_us` and `last_us` are the oldest and the newest sample the window selected, before
the top-k kept any of it (0 and 0 when it selected none); `MEMBERS_ONLY` and `MATCH_LANE` reads, which look
only for particular cells, report the span of the rows they return. A read of an absent key is an empty
answer with status OK. A pair table answers `pair`,
a list table `list`; `timestamp_us` is the newest sample holding the member.

**Errors.** Refusals answer INVALID_ARGUMENT, an unknown table NO_SUCH_TABLE, a duplicate refused
under a no-merge profile DUPLICATE_REFUSED, an exhausted quota QUOTA_EXHAUSTED, anything else ERROR.

**Sample info.** `want_sample_info` (selector field 19) true asks the answer to say which samples the
read used: `sample_info` (answer field 7) holds, oldest sample first, `starts_us` and `ends_us` (a
sample is one instant, so the two are equal) and `member_counts`, the cells each sample held in the
window. The cells are counted before the member set and the predicates apply, and the sample at the
edge of a `NEWEST_ROWS` window counts only the cells the window holds. The list is the samples the answer's
`samples` counts, over every key of a merged read in stamp order. Every read mode takes it. A
`want_sample_info` of false means "not asked", with or without its presence flag, and the answer then
carries no `sample_info` at all.

**Member count (`limit_members`).** `limit_members` (field 12, with `has_limit_members`) is the row
count of a stored-cell (`RECENT`) read that names no `limit` (`limit` of 0 or less): when it is flagged
and positive it is the `limit`. A `limit` that is set wins, and every other read mode ignores the
field. The bound on rows (1 to 1000) applies to the count.

## Where this deliberately differs from the narrower shape

* Unknown table answers NO_SUCH_TABLE (the narrower shape declares the code and answers NO_DATA).
* A custom weighted rank works; the narrower server dereferenced an unset pointer.
* Rank results are ordered; the narrower server returned an unordered selection.
* A batch reports a status per answer, not only the last.
* A request is all or nothing; a partial write is never left behind.
* Every count is bounded; the narrower server bounded none of them.
* A timestamp outside the profile's accepted range is refused, as before; a negative channel or
  kind is refused instead of wrapping into 16 bits.
* `decay` and `decay_rounding` have an effect only in the new `DECAYED_LANE` mode, where they give the
  half-life; in every other mode they are accepted and ignored.

## Bounds

Observations per request, entries per request (existing 1024 cells), queries per batch (64),
members in a member list (1024 each side), quota lanes (16) and quota total (within the top-k cap of
1000), weighted lanes (8), lanes (16), table name bytes (64), top-k (1000), more entities (63, so 64
keys in a merged read), half-life (1 ms to ten years), rows of a `NEWEST_ROWS` window (262,144, and
within 512 samples). Each is refused one past the limit and accepted at it.

## Translated reads, summarised

| Wire | Command |
|---|---|
| one query | `WindowReduceRank { key, spec }` |
| query with `more_entities` | `WindowReduceFanInRank { keys, spec }` |
| batch of queries | `WindowReduceRankBatch { queries }` |
| `members` / `excluded_members` | `spec.members.include` / `spec.members.exclude` |
| newest-anchored window kinds | `spec.anchor = Newest` |
| `quota_lanes`, `quota_counts` | `spec.quota` |
| `DECAYED_LANE`, `decay`, `decay_rounding` | `WindowReduceScore::Decay { lane, half_life_ms }` |
| `RECENT` | `WindowReduceScore::Recent` |
| `MATCH_LANE`, `match_lane`, `match_value` | `WindowReduceScore::Recent` and the predicate `lane = match_value` |

Every translated spec is also checked by the same admission the commands run (`validate_spec`), so a
request the translator accepts is one the engine accepts, and a bound is refused here at the same
number it is refused there.

## Removing cells

`REMOVE` and `DELETE` are one command, `Command::WindowReduceRemove { key, spec }`, which takes cells out of the
samples of one key by time window, member set and predicate, and answers what it removed. It is
general over any key spec: the key is the stored key a table's entity, channel and kind name, and the
names a predicate may use are read from the key's own header.

| Wire | `WindowReduceRemoveSpec` |
|---|---|
| `REMOVE` `span` (microseconds) | `start_ms`, `end_ms`: the stamps inside it, start rounded up and end rounded down, both inclusive; no span is every sample |
| `REMOVE` `key_filter.label` | `filters = [label = value]`, present only when the key filter is |
| `REMOVE` `key_filter.kind`, `.channel` | name the key |
| `REMOVE` `members`, `excluded_members` (additive) | `members.include`, `members.exclude` |
| `DELETE` `selector.label`, `.members`, `.excluded_members` | the same predicate and member set over every sample |
| `DELETE` `selector.channel`, `.kind` | name the key |

A cell is removed when its sample is inside the window, its member passes the member set (`include`
names the members to take, `exclude` the members to spare, a member in both is spared) and every
predicate (`=`, `!=`, `>`, `>=`, `<`, `<=` over a label, a lane or `member`, at most 8, ANDed) holds on
it. A `REMOVE` that restricts nothing is refused with a message that says it would empty the key. A
`DELETE` that restricts nothing (a table with no label, and no member named) deletes the whole key in
one log record, reading no sample, and its receipt counts samples but not cells; a `DELETE` of a key
that holds no sample answers NO_DATA. A sample left with no cell is removed,
so a removal can leave a key that holds no sample; it still reads as an empty answer and takes a
write.

The removal is planned at admission, before the log, by the same function that runs when the entry is
applied: a refused request writes nothing, and a follower or a replay reaches the bytes the leader
planned, since the plan depends on the request and the stored series only (no clock). The series is
replaced in one step from the first sample that changes to the last; a sample in that range that
lost nothing is written back as the bytes it had. A request that matches no cell writes nothing.

Bounds, each refused one past the limit and accepted at it: 1024 samples inside the window (counted
before any is read; narrow the window and remove in steps), 8 predicates, 1024 members a side, and a
bytes member of at most 256 bytes.

The receipt is `samples_before` and `samples_after` over the whole series, `cells_before` over the
samples inside the window, and `cells_removed`. On the wire they are the additive fields 16 to 19 of
`WindowReduceRemoveResponse` and `WindowReduceDeleteResponse`; the established fields are unchanged. The command's
`CommandResponse` carries them as `samples_before`, `count` (the samples after), `cells_before` and
`cells_removed` (field 16).

## Owned elsewhere

Rolling old samples up in tiers is built by its own change and is not translated here.
