#!/usr/bin/env bash
set -euo pipefail
cd /opt/coretelecom/aws_infrastructure


terraform init -input=false
terraform plan -out=tfplan -input=false
# Apply only if TF_VAR_APPLY=true or a confirm env variable is passed
if [ "${TF_AUTO_APPLY:-false}" = "true" ]; then
terraform apply -input=false tfplan
else
echo "TF_AUTO_APPLY not set to true. Skipping terraform apply. To auto-apply set TF_AUTO_APPLY=true"
fi


# Minimal exit
exit 0