#!/usr/bin/env bash
# Explicit --start is required. Default is configuration validation; never auto-restarts.
set -euo pipefail
BASE=/home/huanglabspk/WaiTung
NAME=waitung-vllm-qwen36
WATCH=waitung-vllm-watchdog
OWNER=waitung-qwen36
IMAGE=nvidia/cuda@sha256:0b3589255271d7709c3bc93a37611f43b03a4a3dcc3c72d20459319b4cc27113
PYTHON="$BASE/.venv-vllm/bin/python"
MODEL=/home/huanglabspk/Liangshuyu/freetoken-spark/work/Qwen3.6-35B-A3B
MODE=${1:---validate}
[[ $# -eq 0 ]] || shift
# All flags are explicit; --start only may launch weights. 64K is syntactic only.
CONTEXT_WINDOW=8192
DETAILED_EVENTS=1
KV_CACHE_MEMORY_BYTES=''
ORIGINAL_FLAGS=("$@")
while [[ $# -gt 0 ]]; do
 case "$1" in
  --context-window) [[ $# -ge 2 ]] || exit 2;CONTEXT_WINDOW="$2";shift 2;;
  --detailed-events) [[ $# -ge 2 ]] || exit 2;DETAILED_EVENTS="$2";shift 2;;
  --kv-cache-memory-bytes) [[ $# -ge 2 ]] || exit 2;KV_CACHE_MEMORY_BYTES="$2";shift 2;;
  *) echo "Unrecognized flag: $1" >&2;exit 2;;
 esac
done
case "$CONTEXT_WINDOW" in 8192|16384|32768|65536);;*) echo 'Invalid window' >&2;exit 2;;esac
[[ "$DETAILED_EVENTS" == 0 || "$DETAILED_EVENTS" == 1 ]] || exit 2
[[ -z "$KV_CACHE_MEMORY_BYTES" || "$KV_CACHE_MEMORY_BYTES" =~ ^[1-9][0-9]*$ ]] || exit 2
VLLM_ARGS=(--model /model --served-model-name Qwen3.6-35B-A3B --dtype bfloat16
 --host 0.0.0.0 --port 8000 --tensor-parallel-size 1 --max-model-len "$CONTEXT_WINDOW"
 --gpu-memory-utilization 0.70 --max-num-seqs 1 --max-num-batched-tokens 1024
 --reasoning-parser qwen3 --enable-auto-tool-choice --tool-call-parser qwen3_coder
 --language-model-only --enforce-eager --kernel-config '{"moe_backend":"triton","enable_flashinfer_autotune":false}')
[[ -z "$KV_CACHE_MEMORY_BYTES" ]] || VLLM_ARGS+=(--kv-cache-memory-bytes "$KV_CACHE_MEMORY_BYTES")
if [[ "$DETAILED_EVENTS" == 1 ]]; then
 VLLM_ARGS+=(--middleware agent_traffic_v3.AgentTrafficMiddleware)
fi
ENV_ARGS=(-e PATH="$BASE/.venv-vllm/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
 -e MAX_JOBS=2 -e NVCC_THREADS=1 -e FLASHINFER_NVCC_THREADS=1
 -e OMP_NUM_THREADS=2 -e OPENBLAS_NUM_THREADS=2 -e VLLM_USE_FLASHINFER_SAMPLER=0
 -e HOME=/runtime/home -e HF_HUB_OFFLINE=1 -e TRANSFORMERS_OFFLINE=1
 -e XDG_CACHE_HOME=/runtime/cache -e VLLM_CACHE_ROOT=/runtime/cache/vllm
 -e CUDA_CACHE_PATH=/runtime/cache/cuda -e PYTHONDONTWRITEBYTECODE=1)
if [[ "$DETAILED_EVENTS" == 1 ]]; then
 ENV_ARGS+=(-e AGENT_TRAFFIC_V3_EVENTS=1 -e AGENT_TRAFFIC_V3_DIR=/runtime/v3_telemetry -e PYTHONPATH=/safety-scripts)
fi
MOUNTS=(-v "$BASE/.venv-vllm:$BASE/.venv-vllm:ro"
 -v /home/huanglabspk/miniconda3:/home/huanglabspk/miniconda3:ro
 -v "$MODEL:/model:ro" -v "$BASE/scripts:/safety-scripts:ro")
if [[ "$DETAILED_EVENTS" == 1 ]]; then
 # Mounted file overlays of the pinned 0.24.0 sources. Do not mutate the venv.
 # Compute the Python path dynamically: the supplied /metrics sample shows 3.13.
 VENV_MINOR=$("$PYTHON" -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')
 PACKAGE_DIR="$BASE/.venv-vllm/lib/python$VENV_MINOR/site-packages/vllm"
 [[ -f "$BASE/scripts/agent_traffic_v3.py" && -d "$BASE/vllm_overlay/vllm" ]] || { echo 'Missing V3 overlay/telemetry module' >&2;exit 1; }
 # Verify exact original file bytes before any mount; never overlay a different vLLM version.
 python3 - "$PACKAGE_DIR" "$BASE/vllm_overlay/manifest.json" <<'PYCHK'
import json,sys,pathlib,hashlib
package=pathlib.Path(sys.argv[1]);manifest=json.loads(pathlib.Path(sys.argv[2]).read_text())
for subpath,expected in manifest['original_sha256'].items():
 path=package/subpath
 if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
  raise SystemExit('Source hash mismatch; refusing telemetry overlay: '+str(path))
PYCHK
 while IFS= read -r subpath; do
  [[ -n "$subpath" ]] || continue
  MOUNTS+=(-v "$BASE/vllm_overlay/vllm/$subpath:$PACKAGE_DIR/$subpath:ro")
 done < <(python3 - "$BASE/vllm_overlay/manifest.json" <<'PYLIST'
import json,sys
for p in sorted(json.load(open(sys.argv[1]))['original_sha256']):print(p)
PYLIST
)
fi
case "$MODE" in
 --print-config)
  python3 - "${VLLM_ARGS[@]}" <<'PY'
import json,sys
print(json.dumps({'vllm_args':sys.argv[1:],'memory_bytes':95*1024**3,'memory_swap_total_bytes':95*1024**3,'swap_limit_bytes':0,'compiler_env':{'MAX_JOBS':'2','NVCC_THREADS':'1','FLASHINFER_NVCC_THREADS':'1'},'restart':'no','target_context_window':int(next((sys.argv[i+1] for i,x in enumerate(sys.argv) if x=='--max-model-len'),'0')),'overlay_enabled':'--middleware' in sys.argv},indent=2))
PY
  exit 0;;
 --validate|--start) ;;
 *) echo 'Usage: start_vllm.sh [--validate|--print-config|--start]'; exit 2;;
esac
exec 9>"$BASE/runtime/start-vllm.lock"
flock -n 9 || { echo 'Another start/validation is in progress'; exit 1; }
test "$(stat -fc %T /sys/fs/cgroup)" = cgroup2fs
test "$(docker image inspect -f '{{.Architecture}}' "$IMAGE")" = arm64
# Config validation imports libraries and reads config.json only: no engine, kernels or weights.
PROBE="waitung-vllm-preflight-$(date +%s)-$$"
docker run --rm --pull never --name "$PROBE" --label owner="$OWNER" --label role=preflight \
 --gpus all --network none --memory=2g --memory-swap=2g --cpus=2 --pids-limit=128 \
 --read-only --tmpfs /tmp:rw,nosuid,nodev,size=128m "${ENV_ARGS[@]}" -e HOME=/tmp -e XDG_CACHE_HOME=/tmp/cache -e VLLM_CACHE_ROOT=/tmp/vllm "${MOUNTS[@]}" \
 --entrypoint "$PYTHON" "$IMAGE" /safety-scripts/validate_vllm_config.py "${VLLM_ARGS[@]}" \
 | tee "$BASE/deployment/safety-preflight.latest.log"
[[ "$MODE" == --start ]] || { echo 'Static checks passed; model has NOT been started.'; exit 0; }
# Explicit user invocation of --start is the only branch that may load model weights.
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
GUARD_DIR="$BASE/logs/safety/$RUN_ID"
ARCHIVE="$BASE/deployment/safety-runs/$RUN_ID"
mkdir -p "$GUARD_DIR" "$ARCHIVE"
# Require headroom before removing any stopped container.
python3 - <<'PY'
from pathlib import Path
m={k:int(v.split()[0]) for k,v in (x.split(':',1) for x in Path('/proc/meminfo').read_text().splitlines())}
if m['MemAvailable']<110*1024**2:raise SystemExit('Refusing model launch: need >=110 GiB MemAvailable')
PY
archive_remove_stopped() {
 local n=$1
 if docker container inspect "$n" >/dev/null 2>&1; then
  test "$(docker inspect -f '{{index .Config.Labels "owner"}}' "$n")" = "$OWNER" || { echo "Refusing foreign container: $n"; exit 1; }
  test "$(docker inspect -f '{{.State.Running}}' "$n")" = false || { echo "Refusing to replace running container: $n"; exit 1; }
  docker inspect "$n" > "$ARCHIVE/$n.inspect.json"
  docker logs --timestamps "$n" > "$ARCHIVE/$n.log" 2>&1
  docker rm "$n"
 fi
}
# Validate both ownership and stopped state before mutating either existing container.
for n in "$NAME" "$WATCH"; do
 if docker container inspect "$n" >/dev/null 2>&1; then
  test "$(docker inspect -f '{{index .Config.Labels "owner"}}' "$n")" = "$OWNER"
  test "$(docker inspect -f '{{.State.Running}}' "$n")" = false || { echo "Already running: $n; stop explicitly first"; exit 1; }
 fi
done
archive_remove_stopped "$WATCH"
archive_remove_stopped "$NAME"
cp "$BASE/scripts/start_vllm.sh" "$ARCHIVE/start_vllm.sh"
"$BASE/scripts/start_vllm.sh" --print-config "${ORIGINAL_FLAGS[@]}" > "$ARCHIVE/config.json"
TARGET_ID='';WATCH_ID='';START_OK=0
cleanup() {
 if [[ "$START_OK" != 1 ]]; then
  for id in "$TARGET_ID" "$WATCH_ID"; do
   [[ -n "$id" ]] || continue
   if [[ "$(docker inspect -f '{{index .Config.Labels "owner"}}' "$id" 2>/dev/null)" == "$OWNER" ]]; then docker stop --time 3 "$id" >/dev/null || true; fi
  done
 fi
}
trap cleanup EXIT
TARGET_ID=$(docker create --pull never --name "$NAME" --label owner="$OWNER" --label role=model --label safety-run="$RUN_ID" \
 --restart=no --log-driver local --log-opt max-size=20m --log-opt max-file=5 --gpus all --memory=95g --memory-swap=95g --pids-limit=512 --shm-size=1g \
 -p 127.0.0.1:8000:8000 "${ENV_ARGS[@]}" "${MOUNTS[@]}" \
 -v "$BASE/runtime:/runtime" -v "$GUARD_DIR:/guard:ro" \
 --entrypoint "$PYTHON" "$IMAGE" /safety-scripts/vllm_guarded_entrypoint.py --run-id "$RUN_ID" -- \
 "$PYTHON" -m vllm.entrypoints.openai.api_server "${VLLM_ARGS[@]}")
# Independent watchdog must produce a fresh, safe sample BEFORE docker start.
WATCH_ID=$("$BASE/scripts/start_vllm_watchdog.sh" "$TARGET_ID" "$RUN_ID" "$WATCH")
python3 - "$GUARD_DIR/status.json" "$RUN_ID" "$TARGET_ID" <<'PY'
import json,pathlib,sys,time
path=pathlib.Path(sys.argv[1])
for _ in range(30):
 try:
  s=json.loads(path.read_text())
  if s['state']=='triggered':raise SystemExit('Guard triggered before launch: '+str(s))
  if s['state']=='armed' and s['run_id']==sys.argv[2] and s['target_id']==sys.argv[3] and 0<=time.monotonic()-s['monotonic']<5:break
 except (OSError,ValueError,KeyError):pass
 time.sleep(1)
else:raise SystemExit('No fresh guard readiness: refusing model start')
PY
test "$(docker inspect -f '{{.State.Running}}' "$WATCH_ID")" = true
docker inspect "$TARGET_ID" > "$ARCHIVE/model-created.inspect.json"
docker inspect "$WATCH_ID" > "$ARCHIVE/watchdog.inspect.json"
if [[ -e "$BASE/logs/vllm.log" ]]; then cp -L "$BASE/logs/vllm.log" "$ARCHIVE/previous-vllm.log"; fi
ln -sfn "safety/$RUN_ID/vllm.log" "$BASE/logs/vllm.log"
docker start "$TARGET_ID"
START_OK=1
printf 'Started once, no auto-restart. Model: %s\nGuard: %s\nMetrics: %s/metrics.jsonl\n' "$TARGET_ID" "$WATCH_ID" "$GUARD_DIR"
