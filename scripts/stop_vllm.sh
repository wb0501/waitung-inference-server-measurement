#!/usr/bin/env bash
set -euo pipefail
BASE=/home/huanglabspk/WaiTung
NAME=waitung-vllm-qwen36
OWNER=waitung-qwen36
WATCH=waitung-vllm-watchdog
TARGET_ID=''
if docker container inspect "$NAME" >/dev/null 2>&1; then
 test "$(docker inspect -f '{{index .Config.Labels "owner"}}' "$NAME")" = "$OWNER" || { echo 'Target owner mismatch'; exit 1; }
 TARGET_ID=$(docker inspect -f '{{.Id}}' "$NAME")
 docker stop --time 5 "$TARGET_ID"
fi
# Let the independent guard observe the stopped target and snapshot final logs.
if docker container inspect "$WATCH" >/dev/null 2>&1; then
 test "$(docker inspect -f '{{index .Config.Labels "owner"}}' "$WATCH")" = "$OWNER" || { echo 'Watchdog owner mismatch'; exit 1; }
 test "$(docker inspect -f '{{index .Config.Labels "role"}}' "$WATCH")" = memory-watchdog || exit 1
 test -n "$TARGET_ID" && test "$(docker inspect -f '{{index .Config.Labels "target-id"}}' "$WATCH")" = "$TARGET_ID" || { echo 'Watchdog target mismatch; leaving it untouched'; exit 1; }
 for _ in {1..5}; do
  test "$(docker inspect -f '{{.State.Running}}' "$WATCH")" = true || break
  sleep 1
 done
 docker stop --time 2 "$WATCH"
fi
