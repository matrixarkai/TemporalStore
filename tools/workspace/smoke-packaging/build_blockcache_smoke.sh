#!/usr/bin/env bash
set -euo pipefail
cd /home/vj/tslink
cmake --build build-ubuntu22/release --parallel 4 --target blockcache_smoke > /tmp/temporalstore-blockcache-smoke-build.log 2>&1
tail -100 /tmp/temporalstore-blockcache-smoke-build.log
find build-ubuntu22/release output-ubuntu22/release -maxdepth 6 -type f -name blockcache_smoke -exec ls -lh {} \;
