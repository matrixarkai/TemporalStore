#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Package TemporalStore runtime artifacts from an existing build.

Usage:
  scripts/package_release.sh [--output-dir DIR] [--source-dir DIR] [--name NAME] [--strip]

Defaults:
  --source-dir  newest release-bin-*/output directory, or output/
  --output-dir  ./dist
  --name        temporalstore-runtime-YYYYMMDDHHMMSS

Environment overrides:
  SOURCE_DIR, OUTPUT_DIR, PACKAGE_NAME, STRIP_BINARIES=1

This packages runtime binaries/configuration only. It does not include dependency
source trees, build directories, .git, or existing archives.
USAGE
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="${OUTPUT_DIR:-"$repo_root/dist"}"
source_dir="${SOURCE_DIR:-}"
package_name="${PACKAGE_NAME:-temporalstore-runtime-$(date +%Y%m%d%H%M%S)}"
strip_binaries="${STRIP_BINARIES:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) output_dir="$2"; shift 2 ;;
    --source-dir) source_dir="$2"; shift 2 ;;
    --name) package_name="$2"; shift 2 ;;
    --strip) strip_binaries=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$source_dir" ]]; then
  source_dir="$(find "$repo_root" -maxdepth 2 -type d -path '*/release-bin-*/output' | sort | tail -n 1 || true)"
  [[ -n "$source_dir" ]] || source_dir="$repo_root/output"
fi

if [[ ! -d "$source_dir" ]]; then
  echo "source dir not found: $source_dir" >&2
  echo "pass --source-dir pointing at an existing release output directory" >&2
  exit 1
fi

stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
pkg_root="$stage/$package_name"
mkdir -p "$pkg_root/bin" "$pkg_root/lib" "$pkg_root/conf"

copy_if_exists() {
  local src="$1"
  local dst="$2"
  if [[ -e "$src" ]]; then
    mkdir -p "$(dirname "$dst")"
    cp -a "$src" "$dst"
  fi
}

copy_bin_if_exists() {
  local bin="$1"
  copy_if_exists "$source_dir/$bin" "$pkg_root/bin/$bin"
  copy_if_exists "$source_dir/bin/$bin" "$pkg_root/bin/$bin"
}

for bin in bcache2-metaserver bcache2-server bcache2-proxy bcache2-metaserver-fe stream_tool; do
  copy_bin_if_exists "$bin"
done
find "$pkg_root/bin" -type f -exec chmod +x {} \;

for dir in conf config onebox; do
  copy_if_exists "$source_dir/$dir" "$pkg_root/$dir"
done

if [[ -d "$source_dir/lib" ]]; then
  find "$source_dir/lib" -maxdepth 1 -type f \( -name '*.so' -o -name '*.so.*' \) \
    -exec cp -a {} "$pkg_root/lib/" \;
fi

cat > "$pkg_root/README.txt" <<'README'
TemporalStore runtime package.

Expected services:
  bin/bcache2-metaserver
  bin/bcache2-server
  bin/bcache2-proxy

This archive intentionally excludes source dependencies and build trees.
README

if [[ "$strip_binaries" == "1" ]]; then
  find "$pkg_root/bin" -type f -perm -111 -exec strip --strip-unneeded {} + 2>/dev/null || true
fi

if ! find "$pkg_root/bin" -type f -perm -111 | grep -q .; then
  echo "no executable runtime binaries found under $source_dir" >&2
  exit 1
fi

mkdir -p "$output_dir"
tar -C "$stage" -czf "$output_dir/$package_name.tar.gz" "$package_name"
echo "$output_dir/$package_name.tar.gz"
