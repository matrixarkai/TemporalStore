#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

META_ID="${META_ID:-i-003c930417f7ee609}"
DATA01_ID="${DATA01_ID:-i-0724d90b323786546}"
DATA02_ID="${DATA02_ID:-i-096334bd8cc7ab259}"

PARAMS="$(mktemp)"
cat > "$PARAMS" <<'JSON'
{
  "commands": [
    "set +e\nhostname -f\ndate -u\necho '--- processes ---'\nps -ef | egrep 'kvmaster|kvproxy|tso|partitionserver' | grep -v egrep\necho '--- ports ---'\nss -lntp | egrep '2601|2602|2603|2604' || true\necho '--- recent bytekv logs ---'\nfor f in /var/log/bytekv/master.log /var/log/bytekv/proxy.log /var/log/bytekv/tso.log /var/log/bytekv/partitionserver.log /var/log/bytekv/ps.log /var/log/bytekv/*2026-06-05*.log; do [ -f \"$f\" ] || continue; echo \"### $f\"; tail -n 220 \"$f\"; done\n"
  ]
}
JSON

CMD_ID="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "$META_ID" "$DATA01_ID" "$DATA02_ID" \
  --parameters "file://$PARAMS" \
  --query 'Command.CommandId' \
  --output text)"
rm -f "$PARAMS"

for id in "$META_ID" "$DATA01_ID" "$DATA02_ID"; do
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm wait command-executed --command-id "$CMD_ID" --instance-id "$id" || true
  echo "===== $id ====="
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm get-command-invocation \
    --command-id "$CMD_ID" \
    --instance-id "$id" \
    --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
    --output json
done
