# Contributing to TemporalStore

Thanks for your interest in improving TemporalStore. Issues, discussion and pull
requests are all welcome, from anyone.

## Ground rules

- **Durability rules are not negotiable to make a test pass.** A write is never
  acknowledged before its bytes are durable. If a change moves an `fsync`, it must
  say which barrier still covers the acknowledgement and why.
- **A record's format is a durable contract.** Anything written to disk outlives the
  process that wrote it, so a format change must either be backward compatible
  (a marker or version the reader can detect) or refuse the old shape explicitly and
  fall back to a path that rebuilds. Never make an older file decode into the right
  shape with the wrong contents.
- **Recovery must be provable, not plausible.** If you change replay, compaction or
  a checkpoint, add a test that restarts through the real on-disk artifacts and reads
  every value back.

## Making a change land well

State what the change fixes and how you know. The most useful pull requests here
follow the same shape:

1. **A test that fails first.** Write the test against the current behavior, watch it
   fail, and say what it printed. A test written after the fix proves much less.
2. **A measurement, if the claim is about cost.** "Faster" is hard to review;
   "42 ms of encode moved off the shard write lock" is not. Say what you measured, on
   what corpus, and on what hardware — a loaded shared machine can invent a 3x
   difference that is not there.
3. **The blast radius.** Which callers, which formats, which recovery paths. Grep is
   cheap; a durable format converted at four of its thirteen call sites is not.

## Development

```bash
cargo build --all-targets
cargo test --lib -p temporalstore-rust
cargo fmt --all
cargo clippy --all-targets
```

Some suites are large. When iterating, filter to the area you touched
(`cargo test --lib -p temporalstore-rust -- raft::`), but run the broader suite before
opening the pull request — several subsystems share the engine, and a change to
compaction can surface in the dump or reload tests.

Tests run in parallel and share the machine. A test that pins a process-global (an
environment flag, a fixed port) can fail for reasons that have nothing to do with your
change; re-run the failure on its own before assuming it is yours.

## Reporting a problem

An issue is most actionable with the workload that produced it, what you expected,
what happened, and anything the node logged. Data-loss and durability reports are
prioritized above everything else.
