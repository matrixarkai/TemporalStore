#!/usr/bin/env bash
set -euo pipefail
cd /home/vj/tslink
mkdir -p /tmp/temporalstore-direct-so-src
cat > /tmp/temporalstore-direct-so-src/direct_so_smoke.c <<'C'
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "client/temporalstore_c_client.h"

static int check(int ok, char **err, const char *op) {
  if (ok) return 1;
  fprintf(stderr, "FAIL %s: %s\n", op, (*err ? *err : "unknown"));
  if (*err) temporalstore_free_string(*err);
  *err = NULL;
  return 0;
}

int main(int argc, char** argv) {
  if (argc != 5) {
    fprintf(stderr, "usage: %s <metaserver_host:port> <idc> <namespace> <table>\n", argv[0]);
    return 2;
  }
  temporalstore_options_t options;
  temporalstore_options_init(&options);
  options.metaserver_addr = argv[1];
  options.idc = argv[2];
  options.namespace_name = argv[3];
  options.table_name = argv[4];
  options.psm = "aws.so.direct.smoke";
  char *err = NULL;
  temporalstore_client_t *client = NULL;
  if (!check(temporalstore_connect(&options, &client, &err), &err, "connect")) return 1;
  char key[128];
  snprintf(key, sizeof(key), "direct_so_%lld", (long long)time(NULL));
  if (!check(temporalstore_put_string(client, key, "direct-so-value", &err), &err, "put_string")) return 1;
  char *value = NULL;
  if (!check(temporalstore_get_string(client, key, &value, &err), &err, "get_string")) return 1;
  if (!value || strcmp(value, "direct-so-value") != 0) {
    fprintf(stderr, "FAIL value mismatch: %s\n", value ? value : "null");
    return 1;
  }
  printf("PASS direct libbcache2.so STRING Put/Get key=%s value=%s\n", key, value);
  temporalstore_free_string(value);
  if (!check(temporalstore_close(client, &err), &err, "close")) return 1;
  return 0;
}
C
gcc -Isrc /tmp/temporalstore-direct-so-src/direct_so_smoke.c -Loutput-ubuntu22/release/sdk/lib -lbcache2 -Wl,-rpath,/opt/temporalstore-client-release/sdk/lib -o /tmp/temporalstore-direct-so-src/direct_so_smoke
ldd /tmp/temporalstore-direct-so-src/direct_so_smoke | grep -E 'libbcache2|not found' || true
tar -C /tmp/temporalstore-direct-so-src -czf /tmp/temporalstore-direct-so-smoke.tar.gz direct_so_smoke
ls -lh /tmp/temporalstore-direct-so-smoke.tar.gz
OUTPUT_DIR="${TEMPORALSTORE_SMOKE_OUTPUT_DIR:-/tmp}"
cp /tmp/temporalstore-direct-so-smoke.tar.gz "${OUTPUT_DIR}/temporalstore-direct-so-smoke.tar.gz"
