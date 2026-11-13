# Lazy Expiry Recovery

Command-time expiry removes cached query selectors for each record actually removed from shard
state. Other records, independently live control families, and physical payload-cache entries
are not swept. Query-selector lookup uses the configured record indexes rather than scanning
the whole cache.

## Write Outcomes

A write that observes expired state records its exact removal before recording replacement
blocks. The metadata outcome has the literal kind name `record`, with `deleted` and `meta` set,
and no address, element name, value, or TTL. Recovery rejects malformed combinations.

This differs from the existing `object` deletion outcome, which removes the named record and
its associated control families. An exact removal must not delete a related record whose
deadline has not passed. Existing family-wide deletion outcomes keep their original meaning.

An otherwise losing selection write still records a mutation when it removed expired state.
A losing write that changes nothing remains a no-op.

Reads do not append a new WAL command. The previously recorded deadline remains sufficient to
hide an expired record after restart; a restored physical row must pass the deadline check
before it can answer a query.

## Binary Compatibility

Updated binaries continue reading existing logs. A binary without support for the `record`
outcome cannot recover logs containing that outcome. Binary downgrade after the first such
write is therefore unsupported. The outcome uses the existing named-kind encoding and does
not change the WAL envelope or the replay-aware clock.

## Validation

```sh
cargo test --locked -p temporalstore-rust --lib -- lazy_expiry_cache deadline_source_guards expiry_command_boundaries --test-threads=1
```

The regression cases warm cached fields while a deadline is live, verify sibling fields after
expiry, and restart through the persisted log. They cover partial rewrites, independent family
lifetimes, a losing selection write, and 100,000 unrelated resident payload pages. Syntax-tree
checks cover delegated command handlers and mutation reporting, with negative controls for
discarded results, recursive delegates, and comment or constructor decoys.
