#!/usr/bin/env bash
set -euo pipefail

cd "${TEMPORALSTORE_ROOT:-/home/vj/tslink}"

variant="${TEMPORALSTORE_VARIANT:-release-mtcache-ssd}"
out_dir="output-ubuntu22/${variant}"
build_dir="build-ubuntu22/${variant}"
stage="/tmp/temporalstore-client-tools-${variant}"
debug_stage="/tmp/temporalstore-client-tools-${variant}-debug"
archive="/tmp/temporalstore-client-tools-release.tar.gz"
debug_archive="/tmp/temporalstore-client-tools-release-debug.tar.gz"

rm -rf "$stage"
rm -rf "$debug_stage"
mkdir -p \
  "$stage/bin" \
  "$stage/sdk/lib" \
  "$stage/sdk/include/bcache2" \
  "$stage/examples"
mkdir -p "$debug_stage/bin" "$debug_stage/sdk/lib"

if [ -f "${out_dir}/sdk/lib/libbcache2.so" ]; then
  cp "${out_dir}/sdk/lib/libbcache2.so" "$stage/sdk/lib/"
fi

if [ "${TEMPORALSTORE_PACKAGE_STATIC_SDK:-0}" = "1" ] && \
   [ -f "${out_dir}/sdk/lib/libbcache2.a" ]; then
  cp "${out_dir}/sdk/lib/libbcache2.a" "$stage/sdk/lib/"
fi

if [ -f "${out_dir}/sdk/include/bcache2/bcache2.h" ]; then
  cp "${out_dir}/sdk/include/bcache2/bcache2.h" "$stage/sdk/include/bcache2/"
else
  cp src/client/bcache2.h "$stage/sdk/include/bcache2/"
fi

for tool in \
  "${build_dir}/src/client/example/module_ingest_query_example" \
  "${build_dir}/src/client/example/proxy_smoke_example" \
  "${build_dir}/src/client/example/temporal_aggregate_scale_benchmark" \
  "${build_dir}/src/client/example/temporal_aggregate_lag_sweep"; do
  if [ -x "$tool" ]; then
    cp "$tool" "$stage/bin/"
  fi
done

for src in \
  src/client/example/module_ingest_query_example.cc \
  src/client/example/proxy_smoke_example.cc \
  src/client/example/temporal_aggregate_scale_benchmark.cc \
  src/client/example/temporal_aggregate_lag_sweep.cc; do
  if [ -f "$src" ]; then
    cp "$src" "$stage/examples/"
  fi
done

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

cat >"$stage/README.md" <<'EOF'
# TemporalStore Client Tools

This package contains the client SDK shared library, public header, and small client-side
test/benchmark tools. It intentionally does not include server, metaserver, proxy, or the
static SDK archive unless `TEMPORALSTORE_PACKAGE_STATIC_SDK=1` is set when packaging.
EOF

tar -C "$stage" -czf "$archive" .
if find "$debug_stage" -type f | grep -q .; then
  tar -C "$debug_stage" -czf "$debug_archive" .
  echo "Created $debug_archive"
  du -h "$debug_archive"
fi
echo "Created $archive"
du -h "$archive"
du -ah "$stage" | sort -h | tail -30
