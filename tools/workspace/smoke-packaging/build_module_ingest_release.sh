#!/usr/bin/env bash
set -euo pipefail
cd /home/vj/tslink
cmake --build build-ubuntu22/release --parallel 4 --target module_ingest_query_example > /tmp/temporalstore-release-module-ingest-build.log 2>&1
tail -80 /tmp/temporalstore-release-module-ingest-build.log
ls -lh build-ubuntu22/release/src/client/example/module_ingest_query_example
