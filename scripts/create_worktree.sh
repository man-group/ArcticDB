#!/usr/bin/env bash

set -Eeuo pipefail

BRANCH_NAME=$1
echo "Creating worktree for $BRANCH_NAME out of master"

cd ~/source/ArcticDB
git fetch
git checkout master
git pull
DIR="../$BRANCH_NAME"
git worktree add "$DIR"

echo "Copying over files to new worktree"
cp -r .idea $DIR
cp cpp/CMakeUserPresets.json "$DIR/cpp/"
cp -r cpp/out "$DIR/cpp/"

echo "Updating submodules"
cd "$DIR"
git submodule update --init --recursive

echo "Done"
