#!/usr/bin/env bash
set -euo pipefail

# Validates a deploy target file: the Garden varfile that supplies a remote
# cluster's values (see docs/platform-app-deployment-contract.md).
# Fails fast when required keys are missing or empty.
#
# Usage: validate-deploy-contract.sh [deploy-targets/<target>.yml]
# Defaults to deploy-targets/$DEPLOY_TARGET.yml.

CONFIG_PATH="${1:-deploy-targets/${DEPLOY_TARGET:-}.yml}"

if ! command -v yq >/dev/null 2>&1; then
  echo "ERROR: yq is required for contract validation."
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "ERROR: Deploy target file not found: $CONFIG_PATH"
  exit 1
fi

required_keys=(
  "aws.region"
  "remoteCluster.contextArn"
  "remoteCluster.ecrRegistry"
  "hostname"
  "nfsServerDns"
  "irsa.polarisRoleArn"
  "irsa.prefectJobRoleArn"
  "irsa.xpublishRoleArn"
  "polaris.catalogWarehouse"
  "polaris.catalogRoleArn"
  "icechunk.bucket"
  "pmtiles.bucket"
  "deploy.clusterName"
  "deploy.clusterRoleArn"
  "deploy.actionRoleArn"
)

failures=0

for key in "${required_keys[@]}"; do
  value="$(yq eval ".${key}" "$CONFIG_PATH")"
  if [[ -z "$value" || "$value" == "null" ]]; then
    echo "FAIL: Required key '${key}' is missing or empty in ${CONFIG_PATH}"
    failures=$((failures + 1))
  else
    echo "PASS: ${key} is set"
  fi
done

# The context ARN names the cluster and region too; they must agree.
region="$(yq eval '.aws.region' "$CONFIG_PATH")"
cluster_name="$(yq eval '.deploy.clusterName' "$CONFIG_PATH")"
context_arn="$(yq eval '.remoteCluster.contextArn' "$CONFIG_PATH")"
if [[ "$context_arn" != arn:aws:eks:${region}:*:cluster/${cluster_name} ]]; then
  echo "FAIL: remoteCluster.contextArn (${context_arn}) doesn't match aws.region/deploy.clusterName"
  failures=$((failures + 1))
fi

if [[ "$failures" -gt 0 ]]; then
  echo ""
  echo "Deploy target validation failed with ${failures} issue(s)."
  exit 1
fi

echo ""
echo "Deploy target validation passed."
