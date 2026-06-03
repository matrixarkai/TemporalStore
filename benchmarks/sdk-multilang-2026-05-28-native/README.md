# Multi-language SDK Smoke

Run date: 2026-05-28

## Passed

- Native C++ customer client example
- Native C ABI customer client example
- Shared library rebuild with new exported C ABI helpers
- Python wrapper syntax check with `python3 -m py_compile`

## Covered In Native Smoke

- STRING profile put/get
- HASH feature hset/hget
- SET campaign sadd/smembers
- SEQUENCE feature add/query
- IPS add/query
- RISK increment/window count

## Notes

The local WSL image does not have Go, Java, Maven, or javac installed, so the Go
and Java SDKs were added but not compiled locally.

The Python SDK imports and compiles, but this local debug native shared library is
not safe for in-process Python loading: direct `ctypes` loading reports a static
TLS error, and `LD_PRELOAD` aborts in malloc. A packaged/release shared library or
native sidecar is needed for Python runtime smoke.
