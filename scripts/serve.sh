#!/usr/bin/env bash
# Local static server for the site (required for marked.js CDN; avoid file://).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PORT="${PORT:-8000}"
BIND="${BIND:-127.0.0.1}"
echo "Root: $ROOT"
echo "URL:  http://${BIND}:${PORT}/"
echo "Stop: Ctrl+C"
exec python3 -m http.server "$PORT" --bind "$BIND"
