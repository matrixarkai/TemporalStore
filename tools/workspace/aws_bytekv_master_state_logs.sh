#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
META_ID="${META_ID:-i-003c930417f7ee609}"

PARAMS="$(mktemp)"
cat > "$PARAMS" <<'JSON'
{
  "commands": [
    "set +e\nhostname -f\ndate -u\npython3 - <<'PY'\nfrom pathlib import Path\np=Path('/var/log/bytekv/master.log')\nlines=p.read_text(errors='replace').splitlines() if p.exists() else []\nkeys=['Checking stores state','[STAT] Store','store state','register','Register','report store','heartbeat','fault','FAULT','server','Store:','lost','online']\nfor i,l in enumerate(lines):\n    if any(k in l for k in keys):\n        print(f'{i+1}: {l[:1200]}')\nPY\n"
  ]
}
JSON

CMD_ID="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "$META_ID" \
  --parameters "file://$PARAMS" \
  --query 'Command.CommandId' \
  --output text)"
rm -f "$PARAMS"
aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm wait command-executed --command-id "$CMD_ID" --instance-id "$META_ID" || true
aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm get-command-invocation \
  --command-id "$CMD_ID" \
  --instance-id "$META_ID" \
  --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
  --output json
