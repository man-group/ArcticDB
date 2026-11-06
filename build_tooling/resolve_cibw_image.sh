#!/bin/bash

set -euo pipefail

workflow_hash=$(git hash-object .github/workflows/cibw_docker_image.yml)
script_hash=$(git hash-object build_tooling/build_many_linux_image.sh)
image_ver="$cibuildwheel_ver-$(echo "$workflow_hash $script_hash" | sha1sum | head -c 5)"
image_name="ghcr.io/$(echo "$REPO_OWNER" | tr '[:upper:]' '[:lower:]')/$ARTIFACT_NAME"
export output_tag="$image_name:$image_ver"
export cibuildwheel_ver
found_existing=failure
build_locally=false

if [[ -n "${image_override:-}" ]]; then
    output_tag="$image_override"
    found_existing=success
elif [[ "${force_update:-false}" != true ]]; then
    man_tag="ghcr.io/man-group/$ARTIFACT_NAME:$image_ver"
    if docker manifest inspect "$man_tag" >/dev/null; then
        output_tag="$man_tag"
        found_existing=success
    elif [[ "$output_tag" != "$man_tag" ]] && docker manifest inspect "$output_tag" >/dev/null; then
        found_existing=success
    fi
fi

if [[ "$found_existing" != success ]]; then
    if [[ "${publish_image:-false}" == true ]]; then
        bash build_tooling/build_many_linux_image.sh
        printf '%s' "$GH_TOKEN" | docker login ghcr.io -u token --password-stdin
        docker push "$output_tag"
    else
        # Each wheel runner builds the same image locally, without registry credentials.
        build_locally=true
    fi
fi

printf '%s\n' "tag=$output_tag" "found_existing=$found_existing" "build_locally=$build_locally" >> "$GITHUB_OUTPUT"
