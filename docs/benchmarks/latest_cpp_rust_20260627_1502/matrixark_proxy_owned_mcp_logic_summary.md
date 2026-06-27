# MatrixArk Proxy-Owned MCP Logic Summary

Date: 2026-06-27

## Target Split

Python MCP stays as API, auth, request validation, scope resolution, model orchestration, and parser dispatch. Serving-hot MatrixArk logic moves behind C++/Rust proxy or SDK APIs.

Native C++/Rust proxy responsibilities:

- MatrixArk append and batch append
- prefix/hash shard scan
- secondary-index prefilter
- candidate fetch
- scoring and selected-ref accounting
- token-budget ContextPack assembly
- retrieval telemetry and audit buffering

Python MCP responsibilities:

- resolve identity and access scope
- build the request envelope
- call one native proxy API
- return the finished ContextPack
- run model/parser workers where Python ecosystem is still the right layer

## C++ And Rust Proxy Contract

The proxy contract now includes these MatrixArk endpoints:

- POST /v1/matrixark/append_records
- POST /v1/matrixark/scan_candidates
- POST /v1/matrixark/retrieve_context_pack

Rust already implements the native command path in matrixark_record_log --serve. C++ has the native C ABI/direct SDK implementation and now has the proxy/OpenAPI/Python-client contract needed for proxy-mode serving.

## MCP Wiring

Set MATRIXARK_TEMPORALSTORE_CPP_PROXY_ENDPOINT to route the C++ MatrixArk adapter through the proxy client instead of the embedded direct SDK. In proxy mode, MatrixArk append uses /v1/matrixark/append_records and retrieval uses /v1/matrixark/retrieve_context_pack.

Production/parity policy remains fail-closed:

- native candidate prefilter required
- native ContextPack assembly required
- Python hot record cache disabled
- Python reference packing debug-only

## Remaining Work

The remaining C++ work is implementation of the HTTP handlers behind the new proxy contract if the deployed C++ proxy binary does not yet serve them. The API contract and Python client are now ready, and Rust proxy already owns the equivalent command path.
