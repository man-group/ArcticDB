#!/usr/bin/env bash
#
# Runs the SafeSTSWebIdentityCredentialsProvider test against real AWS.
#
# This script:
#   1. Loads config from setup.sh
#   2. Regenerates the JWT token (in case the old one expired)
#   3. Unsets all other AWS credential env vars so only the web identity path works
#   4. Runs the test via ArcticDB Python API
#
# Usage: ./run_test.sh

set -euo pipefail

WORK_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${WORK_DIR}/test_config.env"

if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "ERROR: ${CONFIG_FILE} not found. Run ./setup.sh first."
    exit 1
fi

# shellcheck source=/dev/null
source "${CONFIG_FILE}"

# Regenerate token in case it expired
echo "==> Regenerating JWT token"
python3 "${WORK_DIR}/generate_token.py" \
    --key "${KEYS_DIR}/oidc-key.pem" \
    --bucket "${OIDC_BUCKET}" \
    --output "${TOKEN_FILE}"

# Clear any credentials that would let earlier providers in the chain succeed.
# We want to force the chain all the way to SafeSTSWebIdentityCredentialsProvider.
echo "==> Clearing AWS credential env vars"
unset AWS_ACCESS_KEY_ID 2>/dev/null || true
unset AWS_SECRET_ACCESS_KEY 2>/dev/null || true
unset AWS_SESSION_TOKEN 2>/dev/null || true
unset AWS_PROFILE 2>/dev/null || true
# Point these at /dev/null so the C++ SDK doesn't read ~/.aws/credentials or ~/.aws/config.
# Just unsetting the env vars isn't enough — the SDK falls back to the default paths.
export AWS_SHARED_CREDENTIALS_FILE=/dev/null
export AWS_CONFIG_FILE=/dev/null

# Set the web identity env vars that SafeSTSWebIdentityCredentialsProvider reads
export AWS_WEB_IDENTITY_TOKEN_FILE="${TOKEN_FILE}"
export AWS_ROLE_ARN="${ROLE_ARN}"
export AWS_ROLE_SESSION_NAME="arcticdb-sts-test"
export AWS_DEFAULT_REGION="${REGION}"

echo "==> Running test"
echo "    Token file:  ${AWS_WEB_IDENTITY_TOKEN_FILE}"
echo "    Role ARN:    ${AWS_ROLE_ARN}"
echo "    Region:      ${AWS_DEFAULT_REGION}"
echo "    Test bucket: ${TEST_BUCKET}"
echo ""

python3 "${WORK_DIR}/test_web_identity.py" \
    --bucket "${TEST_BUCKET}" \
    --region "${REGION}"
