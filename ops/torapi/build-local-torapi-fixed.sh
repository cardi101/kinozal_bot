#!/usr/bin/env bash
set -euo pipefail

PATCH_FILE="/opt/kinozal_bot/ops/torapi/torapi-kinozal-fix.patch"
WORKDIR="/opt/TorAPI-build"
IMAGE_NAME="torapi:kinozal-fix"

if [ ! -f "$PATCH_FILE" ]; then
  echo "Patch file not found: $PATCH_FILE"
  exit 1
fi

rm -rf "$WORKDIR"
git clone https://github.com/Lifailon/TorAPI.git "$WORKDIR"

cd "$WORKDIR"

git apply "$PATCH_FILE"

docker build -t "$IMAGE_NAME" .

echo
echo "Built image: $IMAGE_NAME"
docker images | grep 'torapi' || true
