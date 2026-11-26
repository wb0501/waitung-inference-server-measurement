#!/usr/bin/env bash
set -euo pipefail
BASE=/home/huanglabspk/WaiTung
IMAGE=nvidia/cuda@sha256:0b3589255271d7709c3bc93a37611f43b03a4a3dcc3c72d20459319b4cc27113
TARGET_ID=${1:?target container ID required}
RUN_ID=${2:?run ID required}
WATCH_NAME=${3:?watchdog name required}
TEST=${4:-}
[[ "$RUN_ID" =~ ^[A-Za-z0-9_-]+$ ]] || exit 2
[[ "$WATCH_NAME" == waitung-* ]] || exit 2
test "$(docker inspect -f '{{index .Config.Labels "owner"}}' "$TARGET_ID")" = waitung-qwen36 || exit 1
if [[ -n "$TEST" ]]; then
  test "$(docker inspect -f '{{index .Config.Labels "role"}}' "$TARGET_ID")" = safety-test || exit 1
fi
GUARD_DIR="$BASE/logs/safety/$RUN_ID"
mkdir -p "$GUARD_DIR"
ARGS=()
[[ -z "$TEST" ]] || ARGS+=(--test-scenario "$TEST")
docker run -d --pull never --name "$WATCH_NAME" --label owner=waitung-qwen36 --label role=memory-watchdog --label target-id="$TARGET_ID" \
 --user "$(id -u):$(id -g)" --group-add "$(stat -c %g /var/run/docker.sock)" \
 --log-driver local --log-opt max-size=10m --log-opt max-file=3 \
 --restart=no --memory=256m --memory-swap=256m --cpus=1 --pids-limit=64 \
 --gpus all --network none --read-only --cap-drop ALL --security-opt no-new-privileges \
 --cgroupns=host --tmpfs /tmp:rw,nosuid,nodev,size=16m \
 -e PYTHONDONTWRITEBYTECODE=1 -e NVIDIA_DRIVER_CAPABILITIES=utility \
 -v /var/run/docker.sock:/var/run/docker.sock \
 -v /proc:/host/proc:ro -v /sys/fs/cgroup:/host/cgroup:ro \
 -v /home/huanglabspk/miniconda3:/home/huanglabspk/miniconda3:ro \
 -v "$BASE/scripts:/safety-scripts:ro" -v "$GUARD_DIR:/guard" \
 --entrypoint /home/huanglabspk/miniconda3/bin/python "$IMAGE" \
 /safety-scripts/vllm_memory_guard.py --target-id "$TARGET_ID" --run-id "$RUN_ID" "${ARGS[@]}"
