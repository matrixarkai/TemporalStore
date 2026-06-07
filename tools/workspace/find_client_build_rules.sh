#!/usr/bin/env bash
set +e
cd /home/vj/tslink || exit 1
find . -path './build-ubuntu22' -prune -o -path './output-ubuntu22' -prune -o -type f \( -name 'build-client.sh' -o -name '*.sh' -o -name 'CMakeLists.txt' \) -print > /tmp/ts_files.txt
while IFS= read -r f; do
  grep -nH 'libbcache2\|module_ingest_query_example\|build-client' "$f" 2>/dev/null
done < /tmp/ts_files.txt | head -n 200
