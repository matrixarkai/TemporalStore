#!/usr/bin/env bash
# =============================================================================
# deploy_config.sh - install the centralized config file + launcher on a node
# and wire the running systemd units to read it ON START. Idempotent.
#
# Run this ONCE per node on the NEXT cluster start (the instances persist
# /opt/temporalstore on EBS across stop/start, so this only needs re-running
# when the config file or units change). It:
#
#   1. copies config/temporalstore.toml, scripts/with_config.sh and
#      tools/matrixark_load_config.py from this repo checkout into the canonical
#      /opt/temporalstore tree (never overwrites an existing edited config
#      unless --force-config is given);
#   2. for each installed matrixark-* unit, writes an idempotent drop-in that
#      re-points ExecStart THROUGH the launcher, preserving the unit's current
#      command + args (read live from systemd) so nothing else changes;
#   3. daemon-reload (and --restart to restart the wired units now).
#
# Precedence is preserved: systemd applies a unit's Environment= /
# EnvironmentFile= before ExecStart, so with_config.sh sees those as already-set
# and does not override them (explicit env wins over the config file).
#
# Usage (on the node, as root):
#   sudo /opt/temporalstore/tools/deploy_config.sh [--src <repo>] [--force-config] [--restart]
#   sudo tools/deploy_config.sh                 # from a repo checkout
# =============================================================================
set -euo pipefail

PREFIX="/opt/temporalstore"
CONF_DST="${PREFIX}/config/temporalstore.toml"
DROPIN_NAME="10-with-config.conf"
UNITS=(matrixark-metaserver matrixark-proxy matrixark-datanode matrixark-gateway)

src=""
force_config=0
do_restart=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --src) src="${2:?}"; shift 2 ;;
    --force-config) force_config=1; shift ;;
    --restart) do_restart=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Resolve the repo checkout this script ships in (…/tools/deploy_config.sh).
if [[ -z "$src" ]]; then
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  src="$(cd "${script_dir}/.." && pwd)"
fi

echo "[deploy-config] source repo: ${src}"
for f in config/temporalstore.toml scripts/with_config.sh tools/matrixark_load_config.py; do
  [[ -f "${src}/${f}" ]] || { echo "[deploy-config] MISSING ${src}/${f}" >&2; exit 1; }
done

# --- 1. place config + launcher + loader under /opt/temporalstore -------------
install -d "${PREFIX}/config" "${PREFIX}/scripts" "${PREFIX}/tools"
install -m 0755 "${src}/scripts/with_config.sh"        "${PREFIX}/scripts/with_config.sh"
install -m 0755 "${src}/tools/matrixark_load_config.py" "${PREFIX}/tools/matrixark_load_config.py"
# also drop a copy of this deploy script so it can be re-run from the node.
install -m 0755 "${src}/tools/deploy_config.sh"        "${PREFIX}/tools/deploy_config.sh" 2>/dev/null || true
if [[ -f "$CONF_DST" && "$force_config" -eq 0 ]]; then
  echo "[deploy-config] keeping existing ${CONF_DST} (use --force-config to replace)"
else
  install -m 0644 "${src}/config/temporalstore.toml" "$CONF_DST"
  echo "[deploy-config] installed ${CONF_DST}"
fi

# --- 2. wire each installed unit's ExecStart through the launcher -------------
wired=0
for unit in "${UNITS[@]}"; do
  # only touch units systemd actually knows about on this node.
  if ! systemctl cat "${unit}.service" >/dev/null 2>&1; then
    continue
  fi
  cur="$(systemctl show -p ExecStart --value "${unit}.service" 2>/dev/null || true)"
  [[ -z "$cur" ]] && { echo "[deploy-config] ${unit}: no ExecStart, skipping"; continue; }
  # extract the real command line from: { path=… ; argv[]=<cmd> ; ignore_errors=… }
  argv="$(printf '%s' "$cur" | sed -n 's/.*argv\[\]=\(.*\) ; ignore_errors.*/\1/p')"
  [[ -z "$argv" ]] && argv="$(printf '%s' "$cur" | sed -n 's/.*argv\[\]=\(.*\) ;.*/\1/p')"
  [[ -z "$argv" ]] && { echo "[deploy-config] ${unit}: could not parse ExecStart, skipping"; continue; }
  if printf '%s' "$argv" | grep -q "with_config.sh"; then
    echo "[deploy-config] ${unit}: already wired, skipping"
    continue
  fi
  dropin_dir="/etc/systemd/system/${unit}.service.d"
  install -d "$dropin_dir"
  {
    echo "# Route ExecStart through the config launcher (managed by deploy_config.sh)."
    echo "# Original command is preserved; env still wins over the config file."
    echo "[Service]"
    echo "ExecStart="
    echo "ExecStart=${PREFIX}/scripts/with_config.sh ${CONF_DST} ${argv}"
  } > "${dropin_dir}/${DROPIN_NAME}"
  echo "[deploy-config] ${unit}: wired -> ${dropin_dir}/${DROPIN_NAME}"
  wired=$((wired + 1))
done

# --- 3. reload (+ optional restart) ------------------------------------------
systemctl daemon-reload
echo "[deploy-config] daemon-reload done (${wired} unit(s) newly wired)"
if [[ "$do_restart" -eq 1 ]]; then
  for unit in "${UNITS[@]}"; do
    systemctl cat "${unit}.service" >/dev/null 2>&1 || continue
    systemctl restart "${unit}.service" && echo "[deploy-config] restarted ${unit}"
  done
fi
echo "[deploy-config] done. Services will read ${CONF_DST} on (next) start."
