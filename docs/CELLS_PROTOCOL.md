# Cells protocol

A wire protocol for adding, restoring, removing, deleting and reading merged cell snapshots
(`docs/CELLS_WORKED_EXAMPLE.md`). The messages live in `proto/temporalstore/cells/v1/cells.proto`.
Field numbers and wire types are fixed so that clients speaking this message shape are understood;
every name is ours, and the behaviour is a generalisation: anything the narrower shape could say,
this one says the same way.

## Conventions

* Numbers 1..15 of a message are the established ones. Numbers 16 and up are additive; a reader
  that does not know one skips it.
* Timestamps on the wire are microseconds; the store keeps milliseconds, so a timestamp is floored
  to a millisecond (accepted and documented). Two observations in one millisecond share a snapshot and merge.
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
| CellAddRequest | table, observations, group_on_server, skip_existing, retry_token | 1, 2, 3, 4, 16 | bytes, repeated message, bool, bool, uint64 |
| CellAddResponse | code, message, snapshots_written, cells_created, cells_merged, replayed | 1, 2, 16, 17, 18, 19 | enum, string, uint64 x3, bool |
| Observation | entity, timestamp_us, kind, channel, entries, entity_text, entity_bytes | 1, 2, 3, 4, 5, 16, 17 | int64, int64, int32, int32, repeated message, string, bytes (6 reserved) |
| RestoredObservation | entity, latest_us, earliest_us, kind, channel, entries, entity_text, entity_bytes | 1, 2, 3, 4, 5, 6, 16, 17 | as above (7 reserved) |
| CellEntry | member, shorthand_first, shorthand_second, shape, pair, list, label, has_label, member_bytes | 1..8, 16 | int64, int64, int64, enum, message, message, int32, bool, bytes |
| PairLanes | first, second, timestamp_us | 1, 2, 3 | int64 |
| LaneList | timestamp_us, values | 1, 2 | int64, repeated int64 |
| CellQuery | entity, window, selector, echo, table, has_echo, entity_text, entity_bytes, more_entities | 1, 2, 3, 4, 5, 6, 16, 17, 20 | int64, message, message, bool, bytes, bool, string, bytes, repeated int64 |
| CellBatchQuery | queries | 1 | repeated message |
| CellAnswer | entity, first_us, last_us, entries, snapshot_info, echoed_selector, echoed_window, code, message, snapshots, unscored | 1, 3, 4, 6, 7, 8, 9, 16, 17, 18, 19 | (2 and 5 unused) |
| CellBatchAnswer | answers, code, message | 1, 2, 3 | |
| CellSelector | see the proto | 1..30, 40 | presence flags are separate bools |
| Window | kind, span, start_us | 1, 2, 3 | enum, int64, int64 |

Enum values: operations ADD 0, BATCH_QUERY 1, REMOVE 2, RESTORE 3, DELETE 4. Status codes OK 0,
ERROR 1, STORAGE_ERROR 2, OUT_OF_MEMORY 3, NO_DATA 4, QUOTA_EXHAUSTED 5, REQUEST_ILLEGAL 6,
NO_SUCH_TABLE 7, INVALID_ARGUMENT 8, DUPLICATE_REFUSED 9. Rank modes FIRST_LANE 0, SECOND_LANE 1,
RATIO 2, MEMBER 3, RECENT 4, NAMED_LANE 5, MATCH_LANE 6, WEIGHTED 7, MEMBERS_ONLY 8, WILSON 9.
Window kinds TRAILING_FROM_NOW 0, TRAILING_FROM_NEWEST 1, NEWEST_ROWS 2, FROM_START 3,
TRAILING_FROM_NEWEST_OF_KIND 4 (the same as 1). Rounding FLOOR 0, CEIL 1, ROUND 2.

## Semantics

**Add.** Observations that share an entity, channel, kind and millisecond become one snapshot of
one key; cells with the same member merge lane by lane under the profile's reducers. The whole
request is validated before anything is written: a refusal writes nothing. A list shorter than the
profile's lanes is padded with zeros; a longer one is refused. The shorthand fields 2 and 3 fill a
pair when neither pair nor list is present. A cell with no `has_label` takes its label from the
profile's rule. `skip_existing` is carried but refused for now, with a message pointing at the retry token that makes a
retried add safe; no write mode implements it. `group_on_server` is accepted and always on.

**Restore.** Writes pre-aggregated cells at `latest_us`; `earliest_us` must not exceed it and is
checked only.

**Batch query.** N independent reads, answered in request order, one answer each, each with its own
status. Bounded by the number of queries.

**Windows.** `TRAILING_FROM_NOW` is `[now - span, now]`. `TRAILING_FROM_NEWEST` is the trailing span
ending at the newest stored snapshot of the key. `FROM_START` is `[start, start + span)`.
`NEWEST_ROWS` counts stored (timestamp, member) rows, not snapshots.

**Rank modes.** FIRST_LANE and SECOND_LANE order members by their reduced lane 0 or 1; NAMED_LANE by
the lane at `sort_lane` (zero when absent); RATIO by lane 0 over lane 1; MEMBER by member;
WEIGHTED by the sum of the lanes at `weighted_lanes` times `weights` (indexes and weights not
negative, same length, pair tables only index 0 and 1); WILSON by the lower bound for lane 0
successes of lane 1 trials at `wilson_z` (1.96 when absent). Results are ordered best first; equal
scores order by member. MEMBERS_ONLY returns the listed members and ignores `limit`. RECENT and
MATCH_LANE return stored cells newest first, not reduced per member.

**Answers.** A read of an absent key is an empty answer with status OK. A pair table answers `pair`,
a list table `list`; `timestamp_us` is the newest snapshot holding the member.

**Errors.** Refusals answer INVALID_ARGUMENT, an unknown table NO_SUCH_TABLE, a duplicate refused
under a no-merge profile DUPLICATE_REFUSED, an exhausted quota QUOTA_EXHAUSTED, anything else ERROR.

## Where this deliberately differs from the narrower shape

* Unknown table answers NO_SUCH_TABLE (the narrower shape declares the code and answers NO_DATA).
* `skip_existing` is refused with a clear message; the narrower server never read it.
* A custom weighted rank works; the narrower server dereferenced an unset pointer.
* Rank results are ordered; the narrower server returned an unordered selection.
* A batch reports a status per answer, not only the last.
* A request is all or nothing; a partial write is never left behind.
* Every count is bounded; the narrower server bounded none of them.
* A timestamp outside the profile's accepted range is refused, as before; a negative channel or
  kind is refused instead of wrapping into 16 bits.
* `decay` and `decay_rounding` are accepted with no effect: no behaviour is defined for them. A decaying
  score is a separate, named rank mode.

## Bounds

Observations per request, entries per request (existing 1024 cells), queries per batch (64),
members in a member list (1024 each side), quota entries (sum within the top-k cap of 1000), weighted
lanes (8), lanes (16), table name bytes (64), top-k (1000), more entities (63). Each is refused one
past the limit and accepted at it.

## Owned elsewhere

Idempotent retry (a per-key ring of recent tokens), read filters, anchors, quota and decay, the
fan-in read and rollup are built by their own changes; this layer maps to them.
