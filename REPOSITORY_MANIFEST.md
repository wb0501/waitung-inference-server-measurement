# REPOSITORY_MANIFEST — waitung-inference-server-measurement

推理服务端测量框架（DGX Spark `/home/huanglabspk/WaiTung`）。

- Repository: `waitung-inference-server-measurement`
- Remote path: `/home/huanglabspk/WaiTung`（通过 `ssh spk`）
- vLLM: 0.24.0（插桩 overlay）
- Python（vLLM venv）: 3.13

## 纳入版本管理的文件

### 模型启动与保护

| 文件 | 用途 |
|---|---|
| `scripts/start_vllm.sh` | 安全启动：参数校验、guard、watchdog、容器创建 |
| `scripts/stop_vllm.sh` | 停止模型与 watchdog |
| `scripts/start_vllm_watchdog.sh` | watchdog 启动 |
| `scripts/vllm_memory_guard.py` | MemAvailable / PSI / cgroup OOM 保护 |
| `scripts/vllm_guarded_entrypoint.py` | 容器 PID 1 守卫，检查 guard 状态后 exec vLLM |
| `scripts/vllm_validation_policy.py` | 启动参数策略判定 |
| `scripts/validate_vllm_config.py` | 配置静态校验 |
| `scripts/validate_vllm_controlled.py` | 受控验证 |
| `scripts/validate_vllm_live.py` | 活体验证（HTTP / SSE / metrics） |
| `scripts/monitor_vllm_agent.py` | 轻量监控 |

### 服务端测量

| 文件 | 用途 |
|---|---|
| `scripts/spark_capture.py` | 采集 worker：PCAP、Prometheus、TCP、状态输出 |
| `scripts/agent_traffic_v3.py` | V3 telemetry 写入库（`agent-traffic-v3.0`） |
| `scripts/telemetry_merge.py` | telemetry 分段合并 |
| `scripts/parse_server_capture.py` | 服务端 PCAP 解析 |
| `scripts/experiment_config.py` | 实验配置与上下文 profile |
| `vllm_overlay/manifest.json` | 插桩文件原始/overlay SHA256 清单 |
| `vllm_overlay/vllm-0.24.0-telemetry.patch` | 与未插桩 vLLM 0.24.0 的补丁 |
| `vllm_overlay/vllm/entrypoints/openai/responses/api_router.py` | SSE Encoder 插桩 |
| `vllm_overlay/vllm/entrypoints/openai/responses/serving.py` | Responses API 插桩 |
| `vllm_overlay/vllm/v1/engine/core.py` | EngineCore 插桩 |
| `vllm_overlay/vllm/v1/engine/output_processor.py` | Output Processor 插桩 |
| `vllm_overlay/vllm/v1/core/sched/scheduler.py` | Scheduler 插桩 |

### 文档

| 文件 | 用途 |
|---|---|
| `deployment/enginecore-failure-investigation.md` | Triton/CUDA EPERM 与 EngineCore 启动失败诊断 |
| `deployment/safety-review.md` | 安全启动与 watchdog 审查 |
| `deployment/environment.md` | 服务器环境基线 |

## 明确排除

```text
Models/            模型权重
.venv-vllm/        vLLM 虚拟环境（9 GB）
hf-venv/, miniconda3/   其它运行环境
.cache/, runtime/       缓存与 v3_telemetry 运行数据
logs/              采集日志与 PCAP（26 GB）
guard/             guard 运行时状态
exports/           打包 zip（44 MB）
vllm_source.tar.gz vLLM 源码快照（24 MB）
vllm_metrics.txt   一次性 /metrics 抓取
deployment/before-*/, deployment/safety-*/  历史部署快照
deployment/*.json / *.txt / *.log           运行状态与诊断记录
__pycache__/
```

## 插桩契约

- 插桩方式：Docker 只读 per-file bind mount，不编译扩展、不改镜像
- 环境变量：`PYTHONPATH=/safety-scripts`，中间件 `agent_traffic_v3.AgentTrafficMiddleware`
- telemetry 路径：`/runtime/v3_telemetry/events-<pid>-NNN.jsonl`
- 协议版本：`agent-traffic-v3.0`
- 每个事件带 `response_id` / `request_id` 以便跨层确定性关联

## 与其它仓库的接口

- 被 codex_qwen 与 codex_qwen_GAIA 分别通过 ssh spk 调用：
  `spark_capture.py worker|collect|status`
- `spark_capture.py` / `agent_traffic_v3.py` 在测量仓库中有 vendored 副本用于离线测试；
  本仓库为权威来源，变更后需同步。
