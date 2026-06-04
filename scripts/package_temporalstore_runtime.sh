#!/usr/bin/env bash
set -euo pipefail

cd /home/vj/tslink

stage=/tmp/temporalstore-runtime-release-mtcache-ssd
debug_stage=/tmp/temporalstore-runtime-release-mtcache-ssd-debug
rm -rf "$stage"
rm -rf "$debug_stage"
mkdir -p \
  "$stage/bin" \
  "$stage/lib" \
  "$stage/sdk/lib" \
  "$stage/sdk/include/bcache2"
mkdir -p "$debug_stage/bin" "$debug_stage/sdk/lib"

cp output-ubuntu22/release-mtcache-ssd/bcache2-server "$stage/bin/"
cp output-ubuntu22/release-mtcache-ssd/bcache2-metaserver "$stage/bin/"
if [ -x output-ubuntu22/release-mtcache-ssd/bcache2-proxy ]; then
  cp output-ubuntu22/release-mtcache-ssd/bcache2-proxy "$stage/bin/"
fi
if [ -x build-ubuntu22/release-mtcache-ssd/src/client/example/module_ingest_query_example ]; then
  cp build-ubuntu22/release-mtcache-ssd/src/client/example/module_ingest_query_example "$stage/bin/"
fi

cp build-ubuntu22/release-mtcache-ssd/lib/libthrift.so.0.11.0 "$stage/lib/"
cp /lib/librocksdb.so.6.11 "$stage/lib/" 2>/dev/null || cp /usr/lib/librocksdb.so.6.11 "$stage/lib/"

cp output-ubuntu22/release-mtcache-ssd/sdk/lib/libbcache2.so "$stage/sdk/lib/" 2>/dev/null || true
if [ "${TEMPORALSTORE_PACKAGE_STATIC_SDK:-0}" = "1" ]; then
  cp output-ubuntu22/release-mtcache-ssd/sdk/lib/libbcache2.a "$stage/sdk/lib/" 2>/dev/null || true
fi
cp output-ubuntu22/release-mtcache-ssd/sdk/include/bcache2/bcache2.h "$stage/sdk/include/bcache2/" 2>/dev/null || \
  cp src/client/bcache2.h "$stage/sdk/include/bcache2/"

split_or_strip() {
  local file="$1"
  local rel="${file#$stage/}"
  local debug_file="$debug_stage/$rel.debug"
  if [ "${TEMPORALSTORE_PACKAGE_DEBUG_SYMBOLS:-0}" = "1" ] && command -v objcopy >/dev/null 2>&1; then
    mkdir -p "$(dirname "$debug_file")"
    objcopy --only-keep-debug "$file" "$debug_file" 2>/dev/null || true
    strip --strip-unneeded "$file" 2>/dev/null || true
    objcopy --add-gnu-debuglink="$debug_file" "$file" 2>/dev/null || true
  else
    strip --strip-unneeded "$file" 2>/dev/null || true
  fi
}

while IFS= read -r -d '' file; do
  split_or_strip "$file"
done < <(find "$stage/bin" "$stage/sdk/lib" -type f -perm -111 -print0)

if [ -d tools/temporalstore-monitoring-ui ]; then
  mkdir -p "$stage/monitoring-ui"
  cp -a tools/temporalstore-monitoring-ui/. "$stage/monitoring-ui/"
else
  mkdir -p "$stage/monitoring-ui"
  cat >"$stage/monitoring-ui/index.html" <<'HTML'
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>TemporalStore Monitoring</title></head>
  <body>
    <h1>TemporalStore Monitoring</h1>
    <p>Monitoring UI source was not present in this build tree.</p>
  </body>
</html>
HTML
fi

tar -C "$stage" -czf /tmp/temporalstore-runtime-release-mtcache-ssd.tar.gz .
if find "$debug_stage" -type f | grep -q .; then
  tar -C "$debug_stage" -czf /tmp/temporalstore-runtime-release-mtcache-ssd-debug.tar.gz .
  ls -lh /tmp/temporalstore-runtime-release-mtcache-ssd-debug.tar.gz
fi
ls -lh /tmp/temporalstore-runtime-release-mtcache-ssd.tar.gz
