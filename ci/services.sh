#!/usr/bin/env bash
# Starts what the tests need and waits until it answers: CI runs this before the tests
# (the shared workflow looks for it), and so can you. The emulator image has no shell for a
# healthcheck: its REST port is asked here, then the test instance and database are created.
set -euo pipefail
cd "$(dirname "$0")/.."
testing/gen-keys.sh
docker compose -f testing/compose.yaml up -d --wait
base=http://127.0.0.1:19020/v1
for _ in $(seq 1 60); do
  curl -sf "$base/projects/spelaion/instanceConfigs" >/dev/null && break
  sleep 1
done
curl -s -X POST "$base/projects/spelaion/instances" -H 'content-type: application/json' \
  -d '{"instanceId":"test","instance":{"config":"projects/spelaion/instanceConfigs/emulator-config","displayName":"test","nodeCount":1}}' >/dev/null
curl -s -X POST "$base/projects/spelaion/instances/test/databases" -H 'content-type: application/json' \
  -d '{"createStatement":"CREATE DATABASE demo"}' >/dev/null
curl -sf "$base/projects/spelaion/instances/test/databases/demo" >/dev/null
echo "Spanner emulator ready: projects/spelaion/instances/test/databases/demo"
