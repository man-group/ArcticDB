#!/bin/bash
# Upload vcpkg downloaded assets to S3 cache, keyed by SHA512.
# Usage: vcpkg_upload_assets.sh [DOWNLOADS_DIR]
#   DOWNLOADS_DIR defaults to $VCPKG_ROOT/downloads
if ! command -v aws &>/dev/null; then
    echo "vcpkg asset cache: aws CLI not found, skipping upload"
    exit 0
fi
if [[ -z "$AWS_ACCESS_KEY_ID" ]]; then
    echo "vcpkg asset cache: AWS_ACCESS_KEY_ID not set, skipping upload"
    exit 0
fi

DOWNLOADS="${1:-${VCPKG_ROOT:?VCPKG_ROOT not set}/downloads}"
if ! [[ -d "$DOWNLOADS" ]]; then
    echo "vcpkg asset cache: downloads dir not found at $DOWNLOADS, skipping upload"
    exit 0
fi

BUCKET=arcticdb-ci-vcpkg-assets
REGION=eu-west-1

sha512() { sha512sum "$1" | cut -d' ' -f1; }

uploaded=0
skipped=0
for f in "$DOWNLOADS"/*; do
    [ -f "$f" ] || continue
    [[ "$f" == *.tmp || "$f" == *.part || "$f" == *.lock ]] && continue
    sha=$(sha512 "$f")
    if aws s3api head-object --bucket $BUCKET --key "$sha" --region $REGION &>/dev/null; then
        ((skipped++)) || true
        continue
    fi
    if aws s3 cp "$f" "s3://$BUCKET/$sha" --region $REGION --no-progress 2>/dev/null; then
        ((uploaded++)) || true
    fi
done
echo "vcpkg asset cache: $uploaded new uploads, $skipped already cached"
