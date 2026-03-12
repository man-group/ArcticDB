# STS Web Identity Test Scripts

End-to-end test for `SafeSTSWebIdentityCredentialsProvider` against real AWS.

## Prerequisites

- AWS CLI configured with credentials that can create IAM roles/OIDC providers and S3 buckets
- Python packages: `pip install PyJWT cryptography`

## Usage

```bash
# 1. Set up AWS infrastructure (creates buckets, OIDC provider, IAM role, JWT token)
./setup.sh

# 2. Run the test (regenerates token, unsets other creds, exercises the web identity path)
./run_test.sh

# 3. Clean up everything
./cleanup.sh
```

## What it tests

The test forces `MyAWSCredentialsProviderChain` to reach `SafeSTSWebIdentityCredentialsProvider`
by unsetting all other credential sources (`AWS_ACCESS_KEY_ID`, `AWS_PROFILE`, etc.).

The flow:
1. `EnvironmentAWSCredentialsProvider` -> no creds -> skip
2. `ProfileConfigFileAWSCredentialsProvider` -> no profile -> skip
3. `ProcessCredentialsProvider` -> not configured -> skip
4. **`SafeSTSWebIdentityCredentialsProvider`** -> reads JWT token, calls real STS -> gets temporary creds
5. Uses temporary creds to write/read/delete from S3
