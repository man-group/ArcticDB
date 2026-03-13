#!/usr/bin/env bash
#
# Tears down all AWS resources created by setup.sh.
#
# Usage: ./cleanup.sh

set -euo pipefail

WORK_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${WORK_DIR}/test_config.env"

if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "ERROR: ${CONFIG_FILE} not found. Nothing to clean up."
    exit 1
fi

# shellcheck source=/dev/null
source "${CONFIG_FILE}"

echo "==> Deleting IAM role policy"
aws iam delete-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-name s3-test-access 2>/dev/null || echo "    (already deleted or not found)"

echo "==> Deleting IAM role: ${ROLE_NAME}"
aws iam delete-role \
    --role-name "${ROLE_NAME}" 2>/dev/null || echo "    (already deleted or not found)"

echo "==> Deleting IAM OIDC provider"
aws iam delete-open-id-connect-provider \
    --open-id-connect-provider-arn "${OIDC_PROVIDER_ARN}" 2>/dev/null || echo "    (already deleted or not found)"

echo "==> Deleting OIDC config bucket: ${OIDC_BUCKET}"
aws s3 rb "s3://${OIDC_BUCKET}" --force 2>/dev/null || echo "    (already deleted or not found)"

echo "==> Deleting test data bucket: ${TEST_BUCKET}"
aws s3 rb "s3://${TEST_BUCKET}" --force 2>/dev/null || echo "    (already deleted or not found)"

echo "==> Removing local keys and config"
rm -rf "${KEYS_DIR}"
rm -f "${CONFIG_FILE}"

echo ""
echo "=== Cleanup complete ==="
