# Qwen3.6 BF16 startup safety review ? 2026-11-23

No model was started during this review. The original waitung-vllm-qwen36 remains stopped. The user must explicitly invoke scripts/start_vllm.sh --start to authorize the next load. Running start_vllm.sh without arguments performs static validation only.

## Preserved evidence and prior failure

Original scripts, complete docker logs, and docker inspect are archived under deployment/safety-20261123T103537Z/. Original container state: OOMKilled=false, ExitCode=137. The second attempt completed all 26 BF16 shards, reporting 64.69GiB model allocation at 09:17:27 UTC. The next log entry was termination at 09:57:27 UTC. No explicit CUDA OOM, compiler failure, actual ninja command, compiler process count, or memory trace is present in that log. The final KeyboardInterrupt("terminated") records shutdown and is not evidence of the original hang. Kernel journal query for the relevant interval returned no entries. Therefore neither OOM nor compiler concurrency is proven as the historical root cause. ExitCode 137 alone does not identify the sender of SIGKILL or establish OOM.

## Modified model configuration

- BF16 / original Qwen3.6-35B-A3B read-only mount / TP=1 / GB10.
- --gpu-memory-utilization 0.70
- --max-model-len 8192
- --max-num-batched-tokens 1024
- --max-num-seqs 1
- --language-model-only --enforce-eager
- --reasoning-parser qwen3 --enable-auto-tool-choice --tool-call-parser qwen3_coder
- --kernel-config '{"moe_backend":"triton","enable_flashinfer_autotune":false}'
- VLLM_USE_FLASHINFER_SAMPLER=0 avoids FlashInfer sampling as well as the forced-away FlashInfer CUTLASS MoE path.
- MAX_JOBS=2, NVCC_THREADS=1, FLASHINFER_NVCC_THREADS=1; OMP_NUM_THREADS=2 and OPENBLAS_NUM_THREADS=2.
- Docker --memory=95g --memory-swap=95g --shm-size=1g --pids-limit=512 --restart=no.
- Host publication remains 127.0.0.1:8000. Offline model loading; original dependencies and weights remain read-only. No package, system network, FreeToken, or other-user environment changes.

Startup checks image ARM64 and cgroup v2, runs a 2GiB-limited static preflight, and requires >=110GiB host MemAvailable before model creation. It refuses to replace any running container or any container without owner=waitung-qwen36. A stopped owned model/watchdog is archived and removed, then recreated from the new configuration. It never starts an old configuration in place. Docker start is used only for the newly created ID after its independent guard is ready. No restart loop.

## Static compatibility and compiler control

scripts/validate_vllm_config.py exercises the installed vLLM 0.24.0 OpenAI CLI parser, serve-argument validator, AsyncEngineArgs, KernelConfig, and decoder-only SchedulerConfig. It builds only a small FusedMoEConfig matching the model's BF16 dtype, SILU, 256 experts, top-8, hidden_size=2048, intermediate_size=512, renormalized routing and no TP/DP/EP parallelism. The real installed unquantized-backend oracle returns TRITON / TritonExperts. No model/engine is instantiated and no weights or GPU kernels are executed by this final preflight. This establishes static acceptance, not successful model prefill/decode or all runtime shape paths.

Installed FlashInfer jit/cpp_ext.py: _get_num_workers reads MAX_JOBS; run_ninja appends -j 2. get_nvcc_parallelism_flags reads FLASHINFER_NVCC_THREADS, not NVCC_THREADS, and returns --threads=1. The test mocks subprocess.run and asserts the actual installed function's generated command; it does not compile a large MoE module. NVCC_THREADS=1 is retained for other build systems, but is not claimed to control FlashInfer directly. MAX_JOBS is per Ninja invocation, not a global limit over every process or library.

An attempted stronger model-registry configuration inspection initialized Triton's CUDA C helper and could not dlopen it on the intentionally noexec preflight tmpfs. That path was removed from final preflight to keep it static; it was not a model-loading failure. Normal model runtime uses its writable cache, not that noexec validation tmpfs. The final static validator passes.

## Memory fence and independent monitoring

On a real sleeping ARM64 container, 95g/95g produced memory.max=102005473280 and memory.swap.max=0 under /sys/fs/cgroup/system.slice/docker-<id>.scope. Actual memory.current and memory.events were read. This verifies CPU/memcg configuration and zero additional cgroup swap allowance. It does not prove every NVIDIA driver allocation or all GPU-managed/shared/pinned/unified memory is charged to that cgroup. gpu-memory-utilization=0.70 is an engine budget, not a host-wide physical-RAM limit.

The independent waitung-vllm-watchdog Docker container is started BEFORE the model. It has 256MiB memory, no swap, one CPU, read-only rootfs, no network, dropped capabilities, the existing user's UID and Docker socket group, and no restart policy. It survives SSH session termination. The Docker socket still grants daemon-level power; the code restricts mutating calls to the exact target ID and revalidates owner=waitung-qwen36 each time. It never stops containers by broad name or label enumeration.

Every ~2 seconds it records host MemAvailable, SwapUsed/Total, CPU and memory PSI, loadavg, target cgroup memory.current/peak/max/swap and memory.events; it queries nvidia-smi about every 10 seconds with a 3-second timeout. GB10 memory.used/total can be N/A; host MemAvailable is authoritative for this guard. Root-cgroup memory.events is absent on this host and recorded null; target-cgroup events are available and recorded.

Stop triggers:
- Host MemAvailable <20GiB on any sample.
- Memory PSI full avg10 >=20%, or some avg10 >=60%, continuously for >=15 seconds.
- Target cgroup oom/oom_kill/oom_group_kill event.
- Three consecutive sampling/control failures.

The guard records the reason before attempting Docker stop (3-second grace), then tries kill if still running, with ownership checked again. Its status heartbeat is not refreshed on sampling errors. The model's PID-1 supervisor refuses to launch without a fresh armed heartbeat; it stops its child process group when the heartbeat becomes unsafe or older than 12 seconds. Exiting PID 1 also tears down the container's remaining processes. No automatic restart follows a trigger.

Per-run metrics/status and a periodic tail-2000-line Docker log snapshot are in logs/safety/<run-id>/. logs/vllm.log points to the current snapshot after a controlled launch. Full canonical logs remain in Docker (local driver rotation: model 5x20MB, guard 3x10MB) and are archived before container replacement. If the guard itself fails, inspect docker logs waitung-vllm-qwen36 for the supervisor's reason; a stopped guard cannot write the final snapshot.

## Validation and remaining risks

See deployment/safety-validation.json, deployment/cgroup-limit-validation.txt, deployment/ssh-detach-validation.json and deployment/safety-preflight.latest.log. Synthetic low-memory and PSI inputs are accepted only for targets with both owner=waitung-qwen36 and role=safety-test; they cause real Docker stops of lightweight sleep containers. No real host memory pressure was deliberately created. Watchdog-loss testing actually stops the guard and verifies the target supervisor exits with code 70.

The limits and guard substantially reduce risk but cannot guarantee survival of an abrupt driver/kernel/global-memory stall. Sampling and signal delivery take time; a fully stalled scheduler, daemon, storage or GPU driver may delay both guard and supervisor. Triton still JIT-compiles kernels and GDN/attention/backend fallbacks may allocate additional workspace. The 0.70 budget may fail cleanly with insufficient KV/workspace instead of becoming ready. 8K context, model load, warmup, APIs and inference under this new configuration remain UNTESTED by user instruction. Leave the model stopped until the user chooses the next load.

Next commands:

    ssh spk '/home/huanglabspk/WaiTung/scripts/start_vllm.sh --validate'
    ssh spk '/home/huanglabspk/WaiTung/scripts/start_vllm.sh --print-config'
    ssh spk '/home/huanglabspk/WaiTung/scripts/start_vllm.sh --start'

Only the last command loads the model. To stop explicitly:

    ssh spk '/home/huanglabspk/WaiTung/scripts/stop_vllm.sh'
