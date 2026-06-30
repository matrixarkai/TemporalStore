# Changelog

## 0.1.0

- Added the standalone RustRaft workspace crate.
- Added OpenRaft-free Raft request/response, status, storage, and transport
  contract types.
- Added safety helpers for read-index decisions, append rejection, and learner
  promotion.
- Added semantic parity reporting through `rustraft_parity_report`.
- Added fail-closed production readiness reporting through
  `rustraft_production_readiness_report`.
- Added TemporalStore data-node and metaserver rollout evidence types.
