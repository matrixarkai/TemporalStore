---
description: Show MatrixArk memory backend status and the most recent ingested context.
---

Use the `matrixark` MCP tools to report:

1. Backend readiness and metrics (which TemporalStore backend is active, whether the resident
   node / rust proxy is reachable).
2. The most recent context ingested for this session (counts of events / entities / summaries).

Present it as a short status block. If the backend is unreachable, say so and suggest that
`matrixark_home` is set correctly and the TemporalStore engine is running.
