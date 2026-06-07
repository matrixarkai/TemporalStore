#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
META_ID="${META_ID:-i-003c930417f7ee609}"

PARAMS="$(mktemp)"
cat > "$PARAMS" <<'JSON'
{
  "commands": [
    "set +e\nhostname -f\ndate -u\necho '--- bytekv processes/ports ---'\nps -ef | egrep 'bytekv|kvmaster|kvproxy|partitionserver|tso' | grep -v egrep\nss -lntp | egrep '2601|2602|2603|2604' || true\necho '--- binaries ---'\nfind /opt/bytekv -maxdepth 4 -type f -perm -111 -printf '%p\\n' | sort\necho '--- help samples ---'\nfor b in /opt/bytekv/bin/* /opt/bytekv/bytekv-runtime-release/bin/*; do if [ -x \"$b\" ]; then echo \"### $b\"; timeout 5 \"$b\" --help 2>&1 | head -80; fi; done\necho '--- scripts/docs ---'\nfind /opt/bytekv -maxdepth 5 -type f \\( -name '*.sh' -o -name '*.md' -o -name '*.txt' \\) -printf '%p\\n' | sort | head -80"
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
