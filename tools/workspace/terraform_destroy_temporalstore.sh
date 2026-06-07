#!/usr/bin/env bash
set -euo pipefail

cd /home/vj/temporalstore-tf-destroy
export AWS_PROFILE=temporalstore
export AWS_SDK_LOAD_CONFIG=1

terraform destroy -auto-approve -input=false > terraform-destroy-20260602.log 2>&1
tail -n 120 terraform-destroy-20260602.log
