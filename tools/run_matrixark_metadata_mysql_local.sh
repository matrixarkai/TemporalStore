#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.matrixark-metadata.yml"
export MATRIXARK_MYSQL_PORT="${MATRIXARK_MYSQL_PORT:-3307}"
export MATRIXARK_MYSQL_DATABASE="${MATRIXARK_MYSQL_DATABASE:-matrixark}"
export MATRIXARK_MYSQL_USER="${MATRIXARK_MYSQL_USER:-matrixark}"
export MATRIXARK_MYSQL_PASSWORD="${MATRIXARK_MYSQL_PASSWORD:-matrixark_password}"
export MATRIXARK_MYSQL_ROOT_PASSWORD="${MATRIXARK_MYSQL_ROOT_PASSWORD:-matrixark_root_password}"

cd "$ROOT"
container="matrixark-mysql-metadata"
if docker compose version >/dev/null 2>&1; then
  docker compose -f "$COMPOSE_FILE" up -d
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose -f "$COMPOSE_FILE" up -d
else
  docker network create matrixark-metadata >/dev/null 2>&1 || true
  if docker inspect "$container" >/dev/null 2>&1; then
    docker start "$container" >/dev/null
  else
    docker run -d \
      --name "$container" \
      --restart unless-stopped \
      --network matrixark-metadata \
      -p "${MATRIXARK_MYSQL_PORT}:3306" \
      -e MYSQL_DATABASE="$MATRIXARK_MYSQL_DATABASE" \
      -e MYSQL_USER="$MATRIXARK_MYSQL_USER" \
      -e MYSQL_PASSWORD="$MATRIXARK_MYSQL_PASSWORD" \
      -e MYSQL_ROOT_PASSWORD="$MATRIXARK_MYSQL_ROOT_PASSWORD" \
      -v matrixark_mysql_metadata:/var/lib/mysql \
      "${MATRIXARK_MYSQL_IMAGE:-mysql:8}" >/dev/null
  fi
fi

for _ in $(seq 1 60); do
  state="$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null || true)"
  if [[ "$state" == "healthy" ]]; then
    break
  fi
  sleep 2
done
state="$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null || true)"
if [[ "$state" != "healthy" ]]; then
  docker logs --tail=80 "$container" || true
  echo "MatrixArk MySQL metadata container did not become healthy; state=$state" >&2
  exit 1
fi

export MATRIXARK_METADATA_BACKEND=mysql
export MATRIXARK_METADATA_DSN="mysql://${MATRIXARK_MYSQL_USER}:${MATRIXARK_MYSQL_PASSWORD}@127.0.0.1:${MATRIXARK_MYSQL_PORT}/${MATRIXARK_MYSQL_DATABASE}"
export MATRIXARK_METADATA_AUTO_INIT=1
export MATRIXARK_REQUIRE_SQL_METADATA=1
PYTHONPATH="$ROOT/tools${PYTHONPATH:+:$PYTHONPATH}" python3 "$ROOT/tools/check_matrixark_metadata_sql.py"
cat <<EOF

MatrixArk SQL metadata backend is running locally.
Use these env vars for MCP/HTTP deployment:
export MATRIXARK_METADATA_BACKEND=mysql
export MATRIXARK_METADATA_DSN='mysql://${MATRIXARK_MYSQL_USER}:${MATRIXARK_MYSQL_PASSWORD}@127.0.0.1:${MATRIXARK_MYSQL_PORT}/${MATRIXARK_MYSQL_DATABASE}'
export MATRIXARK_METADATA_AUTO_INIT=1
export MATRIXARK_REQUIRE_SQL_METADATA=1
EOF
