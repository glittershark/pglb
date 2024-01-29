#!/usr/bin/env bash

set -euo pipefail

docker run --rm -v "$(pwd):/app" -w /app rust cargo bench

windtunnel-cli report -f criterion-rust .
