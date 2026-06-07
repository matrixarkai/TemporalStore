#!/usr/bin/env bash
set -euo pipefail

SRC=${SRC:-/home/vj/tslink/output-ubuntu22/release-mtcache-ssd}
REPO_WIN=${REPO_WIN:-/mnt/c/Users/Vincent Jiang/Documents/Codex/2026-05-10/bytekv-in-local-vs-etcd/local_build/BCache2-build-sandbox}
DST=${DST:-${REPO_WIN}/infra/aws/temporalstore-test/artifact-stage}
UI=${UI:-${REPO_WIN}/tools/temporalstore-monitoring-ui}

rm -rf "$DST"
mkdir -p "$DST/bin" "$DST/lib" "$DST/sdk/lib" "$DST/sdk/include/bcache2" "$DST/monitoring-ui"

cp "$SRC/bcache2-server" "$DST/bin/"
cp "$SRC/bcache2-metaserver" "$DST/bin/"
cp "$SRC/bcache2-proxy" "$DST/bin/"
cp /home/vj/tslink/build-ubuntu22/release-mtcache-ssd/lib/libthrift.so.0.11.0 "$DST/lib/"
cp /usr/lib/librocksdb.so.6.11 "$DST/lib/"
cp "$SRC/sdk/lib/libbcache2.so" "$DST/sdk/lib/"
cp "$SRC/sdk/lib/libbcache2.a" "$DST/sdk/lib/"
cp "$SRC/sdk/include/bcache2/bcache2.h" "$DST/sdk/include/bcache2/"
cp -a "$UI/." "$DST/monitoring-ui/"

cat > "$DST/MANIFEST.txt" <<EOF
TemporalStore AWS release artifact
server/metaserver/proxy: release-mtcache-ssd
SDK: libbcache2.so + libbcache2.a
Runtime libs: libthrift.so.0.11.0 + librocksdb.so.6.11
Monitoring UI: static Nginx bundle on metaserver :8088
Created: $(date -Is)
EOF

find "$DST" -maxdepth 4 -type f -printf '%P %s\n' | sort
