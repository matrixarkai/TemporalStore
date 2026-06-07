#!/usr/bin/env bash
set +e
for f in /tmp/temporalstore-proxy-ssd-smoke.launcher.log /tmp/temporalstore-proxy-ssd-smoke/proxy.stderr /tmp/temporalstore-proxy-ssd-smoke/proxy.stdout /tmp/temporalstore-proxy-ssd-smoke/proxy_smoke.log; do
  echo "--- $f"
  if [ -f "$f" ]; then tail -n 160 "$f"; else echo missing; fi
done
echo --- procs
pgrep -af 'proxyssdsmoke|bcache2-proxy' || true
