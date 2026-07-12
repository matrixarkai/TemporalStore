# TemporalStore Open-Source Surface

TemporalStore keeps a smaller public build surface behind
`BCACHE2_OPEN_SOURCE_SURFACE`.

The open-source build keeps:

- Basic Redis-compatible commands: auth/ping/info/config, string commands, key
  lifetime commands, and hash commands.
- MatrixArk context-management data models.
- Feature data model.
- Frequency-control data model.

The open-source build excludes non-public/internal model families and extension
modules such as set, IPS, risk-only, temporal aggregate, and time-series model
registration. The set protobuf may still compile as a compatibility helper for
legacy Redis handler code, but the set module is not registered in the public
surface. Full internal builds remain unchanged when
`BCACHE2_OPEN_SOURCE_SURFACE` is off.

Rust Redis command execution also supports a runtime guard:

- `TEMPORALSTORE_OPEN_SOURCE_SURFACE=1`
- `TS_OPEN_SOURCE_SURFACE=1`

When enabled, unsupported Redis/module commands fail closed before execution.

Run the policy validator after changing this surface:

```bash
python3 tools/validate_open_source_surface.py
```
