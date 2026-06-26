# Client Proxy Control Plane Parity Contract

This shared C++/Rust contract covers behavior exposed by client, proxy, data-node, and
metaserver control-plane paths.

Unified cases should validate:

- topology version changes at table and shard scope
- stale route invalidation and one safe retry for stale-route errors
- admission policy for readonly, write-disabled, drop-percent, degraded, and overload states
- backend route quarantine and recovery probing
- data-node lifecycle states: loading, serving, readonly, reloading, unloading, failed
- metaserver scheduler tokens, generation checks, and stale-token rejection

Rust uses HTTP/JSON, RESP, and tonic surfaces. C++ brpc/thrift behavior can map into this contract
only as behavioral evidence; Rust does not add brpc/thrift transports.
