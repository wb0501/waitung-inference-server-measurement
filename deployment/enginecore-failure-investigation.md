# DGX Spark Qwen3.6-35B-A3B vLLM EngineCore 启动失败诊断

诊断时间：2026-11-25 16:20-16:30 CST（08:20-08:30 UTC）
诊断对象：waitung-vllm-qwen36，容器 ID f141c8e27a91a2a790cd42d7dae49c2e262dc81d5dd4f7cc7b2ad24abbcc6a6c
诊断方式：只读日志、Docker inspect、nvidia-smi、journalctl、/var/log/kern.log、safety metrics；未启动模型、未重启容器、未修改任何启动参数。

## 1. 结论摘要

已确认：

- 模型容器在 2026-11-25T08:14:22Z 左右因 EngineCore 初始化失败退出，ExitCode=1，OOMKilled=false。
- 第一处真正的 vLLM 异常是 Triton 内核加载失败：
  RuntimeError: Triton Error [CUDA]: operation not permitted
- 该错误发生在 Triton 的 driver.active.utils.load_binary -> cuModuleLoadData 路径，不是 Python 参数错误。
- 第一次出现是 GDN prefill warmup 的 warning；紧接着同一个错误在 layernorm Triton kernel 上变成致命错误，导致 EngineCore 无法完成 KV cache profiling。
- 不是内存不足：MemAvailable 最低约 48.7 GB（45.4 GiB），PSI 全程 0，cgroup oom/oom_kill 均为 0。
- 不是 watchdog 主动停止：watchdog 没有 stop_trigger / stop_result，最终状态是 finished，exit_code=1；模型先退出，watchdog 后退出。
- 启动参数、镜像、挂载、内存限制与之前成功的 32K 启动完全一致。

较可能：

- 08:11:21Z 发生了一次 systemd daemon-reload（snapd 自动刷新 gnome-core26 触发），同一秒 cgroup memory.swap.max 从 0 变成 max，随后 08:11:24Z 起 watchdog 的 nvidia-smi 连续报 Failed to initialize NVML: Unknown Error。
- 这些系统级事件与容器内 GPU 访问状态变化时间高度吻合，可能是容器 cgroup/device 策略被重新应用或 NVIDIA 驱动瞬时异常，导致后续 cuModuleLoadData 返回 CUDA_ERROR_NOT_PERMITTED。
- 该机制尚无直接的内核/审计拒绝记录，因此不能称为已证实根因。

尚未确定：

- 为什么 cuModuleLoadData 返回 EPERM，而不是 OOM 或 INVALID_IMAGE。
- 没有发现 Xid、NVRM GPU reset、ECC/RAS、内核 OOM 或 device cgroup denial 记录。
- 当前 nvidia-smi、GPU 可见性测试和 guard-like 容器测试均正常，说明不是持续性的 GPU 硬件故障。

## 2. 精确时间线

时间使用 UTC；括号内为 CST（UTC+8）。

- 08:11:04.101  创建模型容器 f141...。
- 08:11:04.198  创建 watchdog 容器 cb4c...。
- 08:11:05.571  启动模型容器。
- 08:11:08.632  APIServer 开始初始化；确认 enforce-eager、关闭 torch.compile/CUDAGraph。
- 08:11:14.685  EngineCore pid=42 初始化 V1 LLM engine，配置：max_model_len=32768、TP=1、gpu_memory_utilization=0.70、max_num_batched_tokens=1024、max_num_seqs=1、moe_backend=triton、enforce_eager=true。
- 08:11:16.825  选择 FLASH_ATTN 后端，开始加载模型权重。
- 08:11:21.562  systemd 收到 daemon-reload 请求，调用者 PID 1390169，unit=snapd.service（CST 16:11:21）。
- 08:11:22.488  safety metrics 首次记录到容器 cgroup memory.swap.max 从 0 变为 max（此前 8 个样本为 0）。
- 08:11:22.5-22.8  AppArmor profile_replace：snap.chromium 相关 profile（CST 16:11:22）。
- 08:11:24.489  watchdog 第一次 nvidia-smi 失败：Failed to initialize NVML: Unknown Error。
- 08:13:06-08:14:20  权重分片继续加载；26/26 完成。
- 08:14:20.782  权重加载完成，耗时 181.74 s，占用 64.69 GiB。
- 08:14:21.234  设置 attention block size=1056，mamba page size padding 0.76%。
- 08:14:22.620  GDN prefill kernel warmup（T=64）失败：
  qwen_gdn_linear_attn.py:1165
  RuntimeError: Triton Error [CUDA]: operation not permitted
  该异常被 vLLM 当作 warning 捕获，未立即终止。
- 08:14:22.635  EngineCore failed to start；fatal traceback 进入 layernorm Triton kernel，同样在 load_binary 处返回 EPERM。
- 08:14:24.755  APIServer 抛出最终 RuntimeError: Engine core initialization failed. See root cause above. Failed core proc(s): {}。
- 08:14:26.913  容器退出，ExitCode=1，OOMKilled=false。
- 08:14:28.524  watchdog 写入 finished，exit_code=1，随后 watchdog 容器 ExitCode=0。

## 3. vLLM 第一处异常与 EngineCore 直接原因

vllm.log 中第一个非正常事件是 08:14:22.620Z 的 GDN warmup warning：

    WARNING qwen_gdn_linear_attn.py:1165
    GDN prefill kernel warmup (T=64) failed for layer language_model.model.layers.0.linear_attn.
    RuntimeError: Triton Error [CUDA]: operation not permitted

调用栈关键路径：

    qwen_gdn_linear_attn.py:1151 _warmup_prefill_kernels
    fla/ops/chunk.py:61 chunk_gated_delta_rule_fwd
    fla/ops/chunk_delta_h.py:360 chunk_gated_delta_rule_fwd_kernel_h_blockdim64[grid]
    triton/runtime/jit.py:743 launch_metadata
    triton/compiler/compiler.py:465 _init_handles
    driver.active.utils.load_binary
    RuntimeError: Triton Error [CUDA]: operation not permitted

紧接着的致命异常来自同一类路径：

    EngineCore failed to start.
    core.py:134 _initialize_kv_caches
    core.py:284 determine_available_memory
    gpu_worker.py:438 determine_available_memory
    gpu_worker.py:6299 profile_run
    gpu_model_runner.py:5959 _dummy_run
    qwen3_5.py:671 -> qwen3_next.py:585 -> qwen3_next.py:475
    qwen_gdn_linear_attn.py:849/967/866
    layernorm.py:288 forward_cuda
    fla/ops/layernorm_guard.py:227 layer_norm_fwd_kernel[grid]
    triton/runtime/jit.py:743 launch_metadata
    triton/compiler/compiler.py:465 _init_handles
    driver.active.utils.load_binary
    RuntimeError: Triton Error [CUDA]: operation not permitted

结论：EngineCore 是在 KV cache 初始化前的 profile_run 阶段失败；失败点是 Triton 内核模块加载，而不是 KV cache 容量不足、权重加载失败或 Python 配置错误。

## 4. Watchdog 是否参与停止模型

没有参与停止。

证据：

- safety run：logs/safety/20261125T081103Z-1388981
- status.json 最终内容：
  state=finished, exit_code=1, target_id=f141..., utc=2026-11-25T08:14:28.524659Z
- watchdog 日志中：
  stop_trigger 计数 = 0
  stop_result 计数 = 0
  sample_error 计数 = 0
- watchdog 的 103 个样本中，mem_available 最低为 48,706,973,696 字节（约 48.7 GiB），从未低于 20 GiB 阈值。
- memory PSI 的 some/full avg10 全程为 0.0，没有持续 PSI 压力。
- cgroup memory_events 的 oom、oom_kill、oom_group_kill 均为 0。
- Docker events：
  08:14:27.120 模型容器 die, exitCode=1
  08:14:28.576 watchdog die, exitCode=0
  模型先退出，watchdog 后退出。

watchdog 的 NVML 错误只是记录在样本中，不是其 stop 条件。guard 代码只在以下情况主动 stop：
MemAvailable < 20 GiB、持续严重 PSI、cgroup OOM 事件、目标 120 秒未启动、连续 3 次采样异常。本次均未触发。

## 5. CUDA / NVML / Docker Runtime 检查

当前宿主机：

- nvidia-smi：NVIDIA GB10，Driver 580.142，CUDA 13.0，41 C，10 W，0% 利用率。
- GPU 进程：Xorg、gnome-remote-desktop、gnome-shell、WebKitWebProcess，无 vLLM/EngineCore。
- /dev/nvidia0、/dev/nvidiactl、/dev/nvidia-uvm、/dev/nvidia-uvm-tools、/dev/nvidia-caps 均存在。
- nvidia-smi -q：ECC/Retired/Remapped 均为 N/A（GB10），未显示 Xid 或 GPU Reset。
- compute_mode=Default，persistence_mode=Enabled。

隔离容器测试（只运行 nvidia-smi，不启动模型）：

- docker run --rm --gpus all nvidia/cuda:13.0.1-devel-ubuntu24.04 nvidia-smi
  成功，GPU 0: NVIDIA GB10。
- 使用与 watchdog 相近的只读、非 root、256 MiB、--cgroupns=host 参数运行 nvidia-smi
  也成功，四个 /dev/nvidia* 设备可见。

Docker：

- Cgroup Driver: systemd；Cgroup Version: 2；默认 Runtime: runc。
- nvidia-container-cli 1.19.0。
- 模型容器 DeviceRequests=[{Capabilities:[[gpu]], Count:-1}]。
- HostConfig.Memory=102005473280（95 GiB），MemorySwap=102005473280；ShmSize=1 GiB。
- Docker 日志有：
  CDI device injection failed: unresolvable CDI devices nvidia.com/gpu=all
  但该 warning 在之前成功的启动中同样存在，且当前 --gpus all 测试容器 GPU 可见，不能单独作为本次根因。

内核日志：

- journalctl -k --since 2026-11-25 08:10:00Z --until 08:16:00Z 共 21 行，只有 docker0/veth、snap mount、AppArmor profile_replace；没有 NVRM、Xid、GPU reset、RAS、ECC、CUDA、OOM、Killed process、segfault。
- /var/log/kern.log 中本次时间段没有 NVRM 记录。
- 最近一条 NVRM 记录是 2026-11-25T11:37:36+08:00（03:37:36Z）的 _memdescAllocInternal NV_ERR_NO_MEMORY，对应上一轮模型被内存 guard 停止的时间，不是本次 16:11 CST 启动。

## 6. 内存与 GPU 资源变化

safety metrics 共 103 个样本，覆盖 08:11:04.485Z 至 08:14:28.512Z：

- MemAvailable 最大：126,604,537,856 字节（08:11:04，约 117.9 GiB）
- MemAvailable 最小：48,706,973,696 字节（08:12:28，约 48.7 GB / 45.4 GiB）
- memory PSI some/full avg10 最大：0.0
- container cgroup memory.max：102005473280
- container cgroup memory.current 最大：23,792,148,480 字节（约 22.2 GiB）
- container cgroup memory.events：oom=0，oom_kill=0，oom_group_kill=0
- container cgroup memory.swap.current：0
- container cgroup memory.swap.max：
  08:11:06 至 08:11:20 为 0
  08:11:22 起变为 max（共 93 个样本）
- watchdog nvidia-smi 失败：19 次，08:11:24.489Z 至 08:14:24.511Z，均为 Failed to initialize NVML: Unknown Error。

结论：没有触发内存保护；内存和 PSI 不是本次失败原因。

## 7. 与之前成功启动的配置差异

对比对象：20261124T095639Z-3288971（成功启动并运行 32K 模型）与 20261125T081103Z-1388981（本次失败）。

完全一致：

- 镜像：nvidia/cuda@sha256:0b3589255271d7709c3bc93a37611f43b03a4a3dcc3c72d20459319b4cc27113
- config.json：diff 为空
- vLLM 参数：max-model-len=32768、gpu-memory-utilization=0.70、max-num-batched-tokens=1024、max-num-seqs=1、TP=1、enforce-eager、kernel-config moe_backend=triton、--middleware agent_traffic_v3.AgentTrafficMiddleware
- 环境：MAX_JOBS=2、NVCC_THREADS=1、FLASHINFER_NVCC_THREADS=1、OMP_NUM_THREADS=2、VLLM_USE_FLASHINFER_SAMPLER=0、PYTHONPATH=/safety-scripts
- 挂载：.venv-vllm、miniconda3、model、scripts、runtime、guard、五个 vllm_overlay 文件
- HostConfig：Memory=95 GiB、MemorySwap=95 GiB、ShmSize=1 GiB、Runtime=runc、CgroupnsMode=private
- 版本：vLLM 0.24.0、PyTorch 2.11.0+cu130、CUDA 13.0、Triton 3.6.0

本次唯一新增/异常的环境事件：

- snapd 自动刷新 gnome-core26：snap changes ID 98，16:10-16:11 CST。
- systemd daemon-reload：16:11:21.562 CST，调用者 systemctl，unit snapd.service。
- AppArmor profile_replace：16:11:22.5-22.8 CST，snap.chromium 相关 profile。
- cgroup memory.swap.max：08:11:22.488Z 从 0 -> max。
- watchdog NVML：08:11:24.489Z 起失败。
- 上一轮成功启动时（2026-11-24 17:56 CST）没有 snap/systemd reload 事件，watchdog 31,820 个样本中 GPU error = 0。

## 8. 根因分类

### 已确认

1. 直接失败点是 Triton kernel module load：driver.active.utils.load_binary -> cuModuleLoadData 返回 CUDA_ERROR_NOT_PERMITTED。
2. 失败发生在 EngineCore 的 profile_run / KV cache 初始化之前，最终表现为 Engine core initialization failed。
3. 不是 OOM、不是 cgroup 内存上限、不是 PSI、不是 watchdog 主动停止、不是启动参数变化。
4. 当前 GPU 和 Docker GPU 可见性正常，不是持续性 GPU 硬件损坏。

### 较可能

1. 模型启动期间发生系统级 snap 自动刷新 + systemd daemon-reload + AppArmor profile_replace，时间与 cgroup memory.swap.max 重置、NVML 失败高度吻合。
2. 该事件可能重新应用/扰动了容器 cgroup 的 device 权限或 NVIDIA 驱动访问状态，使后续新的 CUDA module load 被拒绝。模型此前已打开设备并完成权重复制，因此能继续加载权重，但后续 Triton 模块加载失败。
3. NVIDIA GB10 驱动在该时段的 NVML Unknown Error 与此一致；当前 nvidia-smi 已恢复。

### 尚未确定

1. 没有内核 Xid、NVRM GPU reset、ECC/RAS 或 device cgroup denial 日志，无法证明 device cgroup 权限被撤销。
2. 不能完全排除 NVIDIA 驱动/GB10 统一内存子系统的瞬时故障；该故障也可能独立于 snap/systemd 事件。
3. 没有在故障时刻进入容器执行 /dev/nvidia* 权限检查，因为容器已退出且不允许重启。

## 9. 最小修复建议（本次未执行）

1. 启动模型前确认 snap 自动刷新已完成：
   snap changes
   等待所有 change 为 Done，或临时设置 refresh.hold，避免 model 启动窗口内发生 snap refresh/systemd daemon-reload。
2. 避免在模型启动或运行期间执行 systemctl daemon-reload、snap refresh、系统包升级。
3. 在 GPU 当前健康的状态下，按原脚本重试一次 32K 启动，不修改任何 vLLM 参数：
   ./scripts/start_vllm.sh --start --context-window 32768 --detailed-events 1
   若重试成功，说明是瞬时环境扰动；不要通过提高 gpu-memory-utilization 或取消 guard 来绕过。
4. 如果再次失败，保留本次 safety metrics 和 vllm.log，检查容器内 /dev/nvidia*、cgroup device 策略，并考虑在一次维护窗口重启宿主机以清除 NVIDIA 驱动状态；不要执行 GPU reset。
5. 不建议修改 Triton MoE、enforce-eager、max-model-len、KV cache 或 MAX_JOBS；本次日志没有证据表明这些参数导致失败。

## 10. 修复后的验证

1. 启动 32K 模型后等待 /health=200，确认 /v1/models 返回 Qwen3.6-35B-A3B。
2. 执行一次短 Responses 生成，确认不是仅 API 启动。
3. 持续观察至少 10 分钟：
   - watchdog metrics 的 gpu.returncode 必须为 0，不能再出现 Failed to initialize NVML。
   - container cgroup memory.swap.max 应保持 0，不应在启动后变成 max。
   - memory.events 的 oom/oom_kill 保持 0。
   - 容器不重启，ExitCode 保持运行状态。
4. 检查 /metrics 的 cache_config_info 确认 32768 上下文和 KV cache 容量。
5. 只有在上述检查通过后，才运行单个 HumanEval/GAIA 任务。

## 11. 证据位置

- logs/vllm.log（当前 symlink 到 logs/safety/20261125T081103Z-1388981/vllm.log）
- logs/safety/20261125T081103Z-1388981/metrics.jsonl
- logs/safety/20261125T081103Z-1388981/status.json
- deployment/safety-runs/20261125T081103Z-1388981/config.json
- deployment/safety-runs/20261125T081103Z-1388981/model-created.inspect.json
- deployment/safety-runs/20261125T081103Z-1388981/watchdog.inspect.json
- /var/log/kern.log
- journalctl（systemd/snapd 日志）
- docker events（容器 die/start 时间）
