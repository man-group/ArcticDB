#!/bin/bash

# Empty S3 settings still select S3 in sccache; leave the unused backend unset.
unset SCCACHE_BUCKET SCCACHE_ENDPOINT SCCACHE_REGION SCCACHE_S3_USE_SSL SCCACHE_GHA_VERSION SCCACHE_GHA_ENABLED
export ACTIONS_CACHE_SERVICE_V2=true
if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    export SCCACHE_BUCKET=arcticdb-ci-sccache-bucket
    export SCCACHE_ENDPOINT=http://s3.eu-west-1.amazonaws.com
    export SCCACHE_REGION=eu-west-1
    export SCCACHE_S3_USE_SSL=false
    export SCCACHE_GHA_ENABLED=false
else
    export SCCACHE_GHA_ENABLED=true
    export SCCACHE_GHA_VERSION="${GHA_CACHE_VERSION:-1}"
fi

if [[ -n "${GITHUB_ENV:-}" ]]; then
    for cache_setting in SCCACHE_BUCKET SCCACHE_ENDPOINT SCCACHE_REGION SCCACHE_S3_USE_SSL SCCACHE_GHA_ENABLED SCCACHE_GHA_VERSION ACTIONS_CACHE_SERVICE_V2; do
        if declare -p "$cache_setting" >/dev/null 2>&1; then
            printf '%s=%s\n' "$cache_setting" "${!cache_setting}" >> "$GITHUB_ENV"
        fi
    done
    unset cache_setting
fi
