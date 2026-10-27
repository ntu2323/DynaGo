#!/usr/bin/env bash
# Run scripts/smoke.sh inside a Linux container (native arch: arm64 on Apple Silicon), so
# the Linux-only transport paths (recvmmsg / sendmmsg, SO_REUSEPORT spreading) run. All
# SMOKE_* / DYNA_* variables are passed through.
#
#   SMOKE_RATE=60000 SMOKE_THREADS=8 scripts/docker-smoke.sh
set -euo pipefail
cd "$(dirname "$0")/.."
ARCH=$(docker info --format '{{.Architecture}}')
case "$ARCH" in aarch64|arm64) GOARCH=arm64 ;; x86_64|amd64) GOARCH=amd64 ;; *) echo "arch $ARCH?"; exit 1 ;; esac
mkdir -p bin/linux
CGO_ENABLED=0 GOOS=linux GOARCH=$GOARCH go build -o bin/linux/ ./cmd/...
envs=()
while IFS='=' read -r k _; do envs+=(-e "$k"); done < <(env | grep -E '^(SMOKE|DYNA)_' || true)
exec docker run --rm --platform "linux/$GOARCH" --ulimit nofile=65536 \
    -v "$PWD:/w" -w /w -e SMOKE_BIN=bin/linux ${envs[@]+"${envs[@]}"} \
    "${SMOKE_IMAGE:-debian:bookworm-slim}" bash scripts/smoke.sh
