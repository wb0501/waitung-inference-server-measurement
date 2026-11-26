

## Safety review 2026-11-23

See [safety-review.md](safety-review.md) for the full preserved-evidence review, exact parameters and memory-accounting limits. Static vLLM 0.24 validation, cgroup 95GiB/no-swap, low-memory stop, sustained PSI stop, heartbeat-loss stop and independent monitoring after SSH disconnect passed. The original model container remains stopped. New 8K model load, warmup and inference are untested. Next model load requires explicit `scripts/start_vllm.sh --start`; no automatic restart.
