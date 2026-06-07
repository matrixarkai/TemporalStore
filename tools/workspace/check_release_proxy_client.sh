#!/usr/bin/env bash
set +e
cd /home/vj/tslink || exit 1
echo '--- tail ---'
tail -n 180 logs/build-release-mtcache-ssd-other-targets.log
echo '--- errors ---'
grep -nEi 'undefined reference|multiple definition|fatal error|error:|ld returned|No rule|Error [0-9]|cannot find' logs/build-release-mtcache-ssd-other-targets.log | tail -n 80 || true
echo '--- artifacts ---'
find output-ubuntu22/release-mtcache-ssd build-ubuntu22/release-mtcache-ssd -type f \( -name 'bcache2-proxy' -o -name 'module_ingest_query_example' -o -name 'libbcache2.so' -o -name 'libbcache2.a' -o -name 'libclient.a' \) -printf '%p %s\n' 2>/dev/null
echo '--- procs ---'
pgrep -af 'cmake --build build-ubuntu22/release-mtcache-ssd' || true
