#!/bin/bash

export VCPKG_ROOT=$(realpath "$(dirname "${BASH_SOURCE[0]}")/../cpp/vcpkg")

pushd "$VCPKG_ROOT"
PLATFORM_VCPKG_ROOT=`cygpath -wa . 2>/dev/null || pwd`

[[ -x vcpkg ]] || ./bootstrap-vcpkg.sh -disableMetrics
export VCPKG_BINARY_SOURCES="clear"

configure_binary_cache() {
    local mode="${VCPKG_BINARY_CACHE_MODE:-readwrite}"
    case "$mode" in
        off) return 0 ;;
        read|readwrite) ;;
        *) echo "Invalid VCPKG_BINARY_CACHE_MODE: $mode" >&2; return 1 ;;
    esac
    if [[ -z "$VCPKG_NUGET_TOKEN" && -z "$VCPKG_MAN_NUGET_TOKEN" ]]; then
        echo "No NuGet credentials; building dependencies from source"
        return 0
    fi
    local nuget=()
    if command -v mono >/dev/null 2>&1; then nuget+=(mono); fi
    local nuget_path
    if ! nuget_path=$(./vcpkg fetch nuget | tail -n 1) || [[ -z "$nuget_path" ]]; then
        echo "NuGet unavailable; building dependencies from source"
        return 0
    fi
    nuget+=("$nuget_path")

    local name user token access url
    for name in github man; do
        if [[ "$name" == github ]]; then
            user="$VCPKG_NUGET_USER"; token="$VCPKG_NUGET_TOKEN"; access="$mode"
            url="https://nuget.pkg.github.com/$user/index.json"
        else
            user="${VCPKG_MAN_NUGET_USER:-$VCPKG_NUGET_USER}"; token="$VCPKG_MAN_NUGET_TOKEN"; access="read"
            url="https://nuget.pkg.github.com/man-group/index.json"
        fi
        [[ -n "$user" && -n "$token" ]] || continue
        # An unavailable feed is a cache miss, never a prerequisite for a build.
        if ! curl --fail --silent --show-error --max-time 30 --user "$user:$token" "$url" >/dev/null; then
            echo "NuGet feed $name unavailable; skipping binary cache"
            continue
        fi
        if ! "${nuget[@]}" sources add -source "$url" -storepasswordincleartext -name "$name" \
            -username "$user" -password "$token"; then
            echo "NuGet feed $name could not be configured; skipping binary cache"
            continue
        fi
        if [[ "$access" == readwrite ]] && ! "${nuget[@]}" setapikey "$token" -source "$url"; then
            echo "NuGet feed $name cannot publish; skipping binary cache"
            continue
        fi
        VCPKG_BINARY_SOURCES="$VCPKG_BINARY_SOURCES;nuget,$name,$access"
    done
}

configure_binary_cache
unset -f configure_binary_cache

popd

# S3 asset cache: caches source archives so builds survive upstream outages
if [[ -n "$AWS_ACCESS_KEY_ID" ]] ; then
    export X_VCPKG_ASSET_SOURCES="x-script,aws s3 cp s3://arcticdb-ci-vcpkg-assets/{sha512} {dst} --region eu-west-1 --only-show-errors"
fi
