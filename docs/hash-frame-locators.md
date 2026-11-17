# Shared Hash Frame Locators

Eligible shared hash batches register each carried frame by its complete physical
address instead of creating one logical WAL locator per selected field. The
authoritative field index still records each field and its address. Multiple
fields can share one frame, and partial updates can keep several frames live for
the same object.

## Publication and Recovery

Bindings validate object identity, resolved address, frame length, and staged
position before publication. An unambiguous ordered batch uses a linear pairing;
ambiguous or incomplete shapes retain generic registration. Block-free metadata
outcomes do not disable otherwise complete hash-frame bindings.

Registration remains under the shard mutation guard. It completes before readers
can observe the batch. The configured durable commit barrier is unchanged; one
caller batch is not split into additional WAL records or synchronization calls.
Replay installs outcomes before publishing their hash-frame locations, so it can
retain compact metadata rather than recreating every field alias.

Existing logical locators remain readable. When an object has physical bindings,
a missing physical address can use a logical locator only after the carrying WAL
record confirms the sequence, exact address, staged position, object, and element.
An unchecked fallback to the latest logical field entry is not allowed. This
mixed-mode recovery path can read a complete carrying record; it is not the normal
physical-hit path.

## Retirement and Memory

Shared publication gathers live addresses only for affected objects. It retires
frames no current hash field needs, including superseded frames within one atomic
record. Rewritten legacy field locators and their obsolete persisted hints are
removed, while untouched siblings remain readable. Full overwrites therefore do
not accumulate a physical version history until background maintenance.

Logical and physical folded-key counts protect collisions without scanning the
whole shard on each write. They are initialized when a shard first introduces
compact hash frames, including during recovery. Initialization walks existing
registrations once under the locator locks; this migration cost is distinct from
steady-state write latency. History-only shards do not maintain these extra maps.

Enabled indexes consume metadata outside the configured payload-cache budget:
one counter per distinct registered folded key, not another copy of the payload.
Empty maps release their capacity but retain a small enabled-state shard header
until shard unload, so later registrations keep collision protection without
repeated initialization. Configured payload capacity is not a process RSS bound.

Ordered frame bindings use their contiguous index coverage directly instead of
allocating a temporary index set. Fully physical batches defer field-name and
packed-object planning until logical registrations actually need migration.
Mixed, reordered, and partial bindings retain exact index membership. Synthetic
WAL offsets still advance before the early return; skipping name planning must
not let recovery reuse an existing synthetic address. Migration name planning
runs outside the global locator mutex.

WAL reclamation continues to use exact log ownership and the existing publication
and recovery fences. A smaller locator count does not authorize deleting records
that another live frame or recovery position still needs. Compare cumulative
writes, synchronization, retained files, metadata allocation, and process memory
separately when qualifying a deployment.

## Upgrade Boundaries

The WAL payload encoding and commit policy are unchanged, but index placements
can use the existing physical-frame key domain for hashes. Deploy supporting
readers before distributing these indexes. Do not assume an older binary can
restore a newer index solely because it understands the WAL encoding. Retain a
consistent pre-upgrade backup until rollback has been qualified for its files.

## Cached Series Reads

History and sequence cache hits decode their typed result payloads directly after
reading the response kind, rather than buffering the complete tagged enum content.
Grouped results retain their order and complete point or row data. The cache
encoder and encoded size are unchanged, as are wire responses, admission policy,
invalidation, and durability. Unsupported response kinds retain the general decoder;
malformed cache entries still cause source recomputation.

Physical read funnels reuse the last shard/slab page-key prefix on each thread.
Only that prefix is retained; selectors still encode the complete offset, length,
and routing bucket. Keys remain byte-identical to the canonical cache constructor,
including after shard changes. No payload, address list, or per-slab map is retained
by this scratch state, and the mutable scratch borrow ends before cache or storage IO.
