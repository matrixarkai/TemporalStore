#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

DATA01_ID="${DATA01_ID:-i-0724d90b323786546}"
DATA02_ID="${DATA02_ID:-i-096334bd8cc7ab259}"

PARAMS="$(mktemp)"
cat > "$PARAMS" <<'JSON'
{
  "commands": [
    "set +e\nhostname -f\ndate -u\ndf -hT\nprintf '\\n--- top dirs / ---\\n'; sudo du -xh / --max-depth=1 2>/dev/null | sort -h | tail -30\nprintf '\\n--- top dirs /var/lib ---\\n'; sudo du -xh /var/lib --max-depth=2 2>/dev/null | sort -h | tail -40\nprintf '\\n--- top dirs /opt ---\\n'; sudo du -xh /opt --max-depth=2 2>/dev/null | sort -h | tail -40\nprintf '\\n--- bytekv data ---\\n'; sudo du -xh /var/lib/bytekv /opt/bytekv /var/log/bytekv 2>/dev/null | sort -h | tail -60\n"
  ]
}
JSON

CMD_ID="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "$DATA01_ID" "$DATA02_ID" \
  --parameters "file://$PARAMS" \
  --query 'Command.CommandId' \
  --output text)"
rm -f "$PARAMS"

for id in "$DATA01_ID" "$DATA02_ID"; do
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm wait command-executed --command-id "$CMD_ID" --instance-id "$id" || true
  echo "===== $id ====="
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm get-command-invocation \
    --command-id "$CMD_ID" \
    --instance-id "$id" \
    --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
    --output json
done
