# Product API C++ Rust Parity Contract

This shared contract covers product-facing command behavior that should not remain as isolated
C++ or Rust tests.

Unified cases should validate:

- Redis/admin command aliases, status codes, and typed errors
- string, hash, set, and common key lifecycle behavior
- Feature nested point/proto semantics, replacement, filters, and aggregation
- Sequence ordering, batch query, and missing-series behavior
- IPS snapshot, stat, filter, and metadata behavior
- Risk CPC, FOL/list, manager, and debug APIs

When a behavior is product-visible, prefer a shared command/response case. Local C++ or Rust tests
should remain only for implementation internals, parser helpers, mocks, and build-specific surfaces.
