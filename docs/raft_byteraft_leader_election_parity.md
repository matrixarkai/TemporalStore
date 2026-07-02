# ByteRaft Leader Election Parity

This page defines the shared Rust/C++ evidence contract for ByteRaft-style
leader election and learner promotion behavior.

## Rust Evidence

Rust emits `ByteRaftLeaderElectionParityReport` from the Raft runtime admin
state. The report is ready only when all of these fields are true:

- `leader_election_ready`: pre-vote is enabled and observed from runtime state.
- `pre_vote_ready`: both no-quorum rejection and quorum acceptance were observed.
- `leader_failover_observed`: leadership moved to a later term leader.
- `learner_add_ready`: learner add evidence exists.
- `learner_catchup_ready`: learner catch-up evidence exists.
- `learner_promote_ready`: learner promotion evidence exists.
- `learner_auto_promote_ready`: auto-promote evidence exists.
- `leader_transfer_exact_once_ready`: a transfer-under-write commit id was recorded once.

The focused Rust test is:

```bash
cargo test -p temporalstore-rust byteraft_leader_election_and_learner_promotion_parity_report_is_ready --lib -- --test-threads=1
```

## Shared Case

The shared corpus case is:

```text
raft_byteraft_leader_election_learner_promotion_parity
```

It covers:

- no-quorum pre-vote rejection
- healed-quorum leader election
- learner add, catch-up, promote
- learner auto-promote
- leader transfer under a write with exact-once commit evidence

## C++ Adapter Contract

The C++ ByteRaft adapter should emit the same case name and equivalent report
fields from ByteRaft/TemporalStore data-raft tests. Until the native C++
adapter emits those fields, the case remains a Rust-executable plus C++ static
surface gate.

## Failure Policy

Any missing field is represented in `blockers`. A parity or production
readiness claim must fail closed when `ready=false`.
