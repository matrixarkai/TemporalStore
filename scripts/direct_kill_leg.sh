#!/bin/bash
# Kills and restarts one server on a PORT while `direct_surface_cluster` runs LEG=append, to
# interrupt appends. The restart command is yours, because the rig that started the server knows
# how: pass it as RESTART. Usage:
#   PORT=24401 RESTART='bash start_chunk.sh' ROUNDS=10 PAUSE=5 scripts/direct_kill_leg.sh
# A server this script restarts is NOT in the pid file of the rig that started the cluster, so that rig's
# `down` will not stop it: stop the restarted servers yourself (by their port) when the run ends.
set -u
PORT=${PORT:?PORT of the server to kill}
RESTART=${RESTART:?command that restarts it}
ROUNDS=${ROUNDS:-10}
PAUSE=${PAUSE:-5}
for round in $(seq 1 "$ROUNDS"); do
  sleep "$PAUSE"
  pid=$(ss -lntp 2>/dev/null | awk -v p=":$PORT" '$4 ~ p"$" {print $NF}' | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2)
  if [ -z "$pid" ]; then echo "round $round: nothing listens on $PORT"; continue; fi
  echo "$(date +%H:%M:%S) round $round: SIGKILL pid $pid on port $PORT"
  kill -9 "$pid"
  sleep 2
  bash -c "$RESTART"
done
