#!/usr/bin/env bash
set -euo pipefail

cd /home/vj/tslink

stage=/tmp/temporalstore-runtime-release-mtcache-ssd
rm -rf "$stage"
mkdir -p \
  "$stage/bin" \
  "$stage/lib" \
  "$stage/sdk/lib" \
  "$stage/sdk/include/bcache2" \
  "$stage/monitoring-ui"

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
cp output-ubuntu22/release-mtcache-ssd/sdk/lib/libbcache2.a "$stage/sdk/lib/" 2>/dev/null || true
cp output-ubuntu22/release-mtcache-ssd/sdk/include/bcache2/bcache2.h "$stage/sdk/include/bcache2/" 2>/dev/null || \
  cp src/client/bcache2.h "$stage/sdk/include/bcache2/"

cat >"$stage/monitoring-ui/index.html" <<'HTML'
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>TemporalStore AWS Test</title></head>
  <body>
    <h1>TemporalStore AWS Test</h1>
    <p>Monitoring placeholder deployed with runtime artifact.</p>
  </body>
</html>
HTML

tar -C "$stage" -czf /tmp/temporalstore-runtime-release-mtcache-ssd.tar.gz .
ls -lh /tmp/temporalstore-runtime-release-mtcache-ssd.tar.gz
