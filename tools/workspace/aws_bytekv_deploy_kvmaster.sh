#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-temporalstore}"
REGION="${AWS_REGION:-us-west-2}"
BUCKET="${ARTIFACT_BUCKET:-temporalstore-test-artifacts-657817560042-us-west-2}"
META="${META_INSTANCE_ID:-i-003c930417f7ee609}"
BIN="${KVM_BIN:-/home/vj/bytekv-rocksdb-server/build-release/stripped/kvmaster-small-internal}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
KEY="one-cluster/byekv-kvmaster-small-internal-${RUN_ID}"

aws --profile "$PROFILE" --region "$REGION" s3 cp "$BIN" "s3://${BUCKET}/${KEY}" >/dev/null
URL="$(aws --profile "$PROFILE" --region "$REGION" s3 presign "s3://${BUCKET}/${KEY}" --expires-in 3600)"

PARAMS="$(mktemp)"
python3 - "$URL" <<'PY' > "$PARAMS"
import json, sys
url = sys.argv[1]
cmd = f'''
bash -lc '
set -eux
mkdir -p /opt/bytekv/bin /opt/bytekv/bin/backups
if [ -x /opt/bytekv/bin/kvmaster ]; then
  cp /opt/bytekv/bin/kvmaster /opt/bytekv/bin/backups/kvmaster.$(date -u +%Y%m%dT%H%M%SZ)
fi
curl -fL "{url}" -o /opt/bytekv/bin/kvmaster.new
chmod 755 /opt/bytekv/bin/kvmaster.new
mv -f /opt/bytekv/bin/kvmaster.new /opt/bytekv/bin/kvmaster
ls -lh /opt/bytekv/bin/kvmaster
ldd /opt/bytekv/bin/kvmaster || true
'
'''
print(json.dumps({"commands":[cmd]}))
PY

cmd_id="$(aws --profile "$PROFILE" --region "$REGION" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "$META" \
  --parameters "file://$PARAMS" \
  --query Command.CommandId \
  --output text)"

aws --profile "$PROFILE" --region "$REGION" ssm wait command-executed \
  --command-id "$cmd_id" \
  --instance-id "$META" || true

aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
  --command-id "$cmd_id" \
  --instance-id "$META" \
  --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
  --output json

rm -f "$PARAMS"
