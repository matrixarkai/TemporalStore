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
cargo test -p temporalstore-rust --lib --tests --no-fail-fast -- --test-threads=1
cargo fmt --all
cargo clippy --all-targets
```

That test line is deliberately the shape continuous integration runs (it adds `--release`),
and each part of it matters:

- **`--tests`** — without it only the library suite runs, and the integration test
  binaries under `tests/` do not. Those are where the restart-through-real-artifacts
  coverage lives, so a recovery change that only passes `--lib` has not been exercised.
- **`--no-fail-fast`** — cargo stops at the first failing *target*, not the first failing
  test. One library failure therefore hides every integration binary behind it, and a run
  has printed two `test result:` lines for a command asked to run every target.
- **`--test-threads=1`** — several suites pin process-global state (an environment flag, a
  fixed port, a working directory). Running them in parallel produces failures that belong
  to the interleaving rather than to your change.

Some modules are entirely `#[ignore]`d, usually because they measure something that needs
the machine to itself. They do not run in the command above, and a filter that matches
nothing exits successfully, so check rather than assume:

```bash
cargo test -p temporalstore-rust --lib -- --ignored --list
```

There is an `alloc-probe` feature that installs a counting allocator in test builds. It is
a measurement tool, **not** a second gate: with it installed, socket-bound proxy and raft
tests go red that pass individually, as a group, and in a run without it. Use it to measure
one thing, not to check your change:

```bash
cargo test -p temporalstore-rust --features alloc-probe --lib <the_one_test> -- --ignored --nocapture
```

### The check mark is a compile gate, not a test gate

The `cargo test` step in `.github/workflows/rust-ci.yml` is marked
`continue-on-error: true`, and the comment above it says why: the compile steps are what
enforce the workflow, and the suite runs for visibility until the baseline is fully green.
So a green check mark on your pull request means the tree **compiled**. It does not mean
your change was tested, and a maintainer may still come back with a failure the check did
not show you.

Run the suite locally, and when something fails, find out whether you caused it before
changing anything:

```bash
git stash && cargo test -p temporalstore-rust --lib --tests --no-fail-fast -- --test-threads=1 2>&1 | tee /tmp/before.txt
git stash pop && cargo test -p temporalstore-rust --lib --tests --no-fail-fast -- --test-threads=1 2>&1 | tee /tmp/after.txt
```

Then compare the **names** that failed, not the counts. A count that matches can hide one
failure appearing while another disappears, and a name that fails identically on an
unmodified tree is not yours to fix in this pull request — say so in the description and
move on. This is the single most useful thing to include when a suite is not green: which
names are new.

Once you are iterating on one area, filter to it
(`cargo test -p temporalstore-rust --lib -- raft::`), but run the full line above before
opening the pull request. Several subsystems share the engine, and a change to compaction
surfaces in the dump and reload tests.

## Reporting a problem

An issue is most actionable with the workload that produced it, what you expected,
what happened, and anything the node logged. Data-loss and durability reports are
prioritized above everything else.
