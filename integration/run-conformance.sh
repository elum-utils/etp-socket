#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$(mktemp)"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -f "$LOG"
}
trap cleanup EXIT INT TERM

(cd "$ROOT/integration/go-server" && go run . -addr 127.0.0.1:18991) >"$LOG" 2>&1 &
SERVER_PID=$!

for _ in {1..100}; do
  if nc -z 127.0.0.1 18991 2>/dev/null; then
    ETP_GO_URL=ws://127.0.0.1:18991 "$ROOT/node_modules/.bin/vitest" run "$ROOT/src/go.integration.test.ts" --reporter=verbose
    exit 0
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$LOG"
    exit 1
  fi
  sleep 0.05
done

cat "$LOG"
echo "Go ETP conformance server did not become ready" >&2
exit 1
