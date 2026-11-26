# waitung-inference-server-measurement

推理服务端测量框架：DGX Spark 上的 vLLM 插桩、跨层 telemetry、资源监控与
模型安全启动。

本仓库属于 **Agent Traffic Measurement** 研究项目，负责把"推理服务器内部
发生了什么"变成可对齐的时间线：API Server、Scheduler、EngineCore、
Output Processor、SSE Encoder 各自的事件，加上 Prometheus 与 TCP 采样。

它与 Agent 侧无关：GAIA、HumanEval 或其他 workload 由
`waitung-agent-workload-runners` 驱动，本仓库只负责服务端。

## 职责

- vLLM 0.24.0 只读 bind-mount 插桩（`vllm_overlay/`）
- V3 telemetry（`agent-traffic-v3.0`）写入与合并
- Prometheus（默认 1 s）与 TCP 采样（`spark_capture.py`）
- 模型安全启动：guard entrypoint、内存保护 watchdog、启动脚本
- 启动前参数校验与活体验证
- GPU / 容器 / 内存 / PSI 诊断与故障调查文档

## 目录

```text
scripts/start_vllm.sh              模型安全启动（参数校验 + guard + watchdog）
scripts/stop_vllm.sh               停止模型与 watchdog
scripts/start_vllm_watchdog.sh     watchdog 启动
scripts/vllm_memory_guard.py       内存/PSI/OOM 保护
scripts/vllm_guarded_entrypoint.py 容器 PID 1 守卫入口
scripts/spark_capture.py           双端采集 worker（PCAP + Prometheus + TCP）
scripts/agent_traffic_v3.py         V3 telemetry 写入库（被 overlay 引用）
scripts/telemetry_merge.py          telemetry 分段合并
scripts/parse_server_capture.py     服务端 PCAP 解析
scripts/experiment_config.py        实验配置与上下文 profile
scripts/validate_vllm_*.py          启动参数与活体验证
scripts/monitor_vllm_agent.py       轻量监控
vllm_overlay/                       vLLM 0.24.0 插桩文件与补丁
deployment/                         安全审查与故障调查文档
```

## vLLM 插桩说明

`vllm_overlay/manifest.json` 记录每个文件的原始 SHA256 与 overlay 后 SHA256，
`vllm-0.24.0-telemetry.patch` 是与未插桩 vLLM 0.24.0 的差异。

插桩方式是 Docker **只读 per-file bind mount**，不编译扩展、不修改镜像：

```text
vllm/entrypoints/openai/responses/api_router.py    SSE Encoder
vllm/entrypoints/openai/responses/serving.py       Responses API
vllm/v1/engine/core.py                             EngineCore
vllm/v1/engine/output_processor.py                 Output Processor
vllm/v1/core/sched/scheduler.py                    Scheduler
```

## 环境要求

- DGX Spark（GB10，ARM64），CUDA 13.0，驱动 580.x
- vLLM 0.24.0，Python 3.13
- Docker + NVIDIA container toolkit
- `dumpcap` / `tshark`（服务端采集）

## 启动方式

```bash
cd /home/huanglabspk/WaiTung
./scripts/start_vllm.sh --start --context-window 32768 --detailed-events 1
```

## 仓库关系

见 `REPOSITORY_MANIFEST.md` 中的依赖说明：

```text
waitung-agent-workload-runners -> waitung-agent-traffic-measurement -> 本仓库
```
