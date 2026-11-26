"""Version-locked, explicit workload profiles; does not assert successful deployment."""
from __future__ import annotations
import dataclasses
import json
import pathlib

PROFILES = {
    'baseline-8k': (8192, 6000),
    'context-16k': (16384, 12000),
    'context-32k': (32768, 24000),
    'context-64k': (65536, 48000),
}
MODEL = 'Qwen3.6-35B-A3B'
STABLE = {'gpu_memory_utilization': .70, 'max_num_batched_tokens': 1024,
          'max_num_seqs': 1, 'dtype': 'bfloat16', 'tensor_parallel_size': 1,
          'enforce_eager': True, 'prefix_caching': False}

@dataclasses.dataclass(frozen=True)
class ExperimentConfig:
    profile: str
    server_context: int
    codex_context: int
    auto_compact_threshold: int
    metrics_interval: float = 1.0
    detailed_events: bool = True
    tcp_interval: float = 1.0
    kv_cache_memory_bytes: int | None = None

    @classmethod
    def build(cls, profile='context-32k', metrics_interval=1., detailed_events=True,
              tcp_interval=1., kv_cache_memory_bytes=None):
        if profile not in PROFILES:
            raise ValueError(f'Unknown profile {profile}; choose {list(PROFILES)}')
        ctx, limit = PROFILES[profile]
        if metrics_interval not in (0, .5, 1., 2., 5.):
            raise ValueError('metrics_interval must be 0, 0.5, 1, 2, or 5 seconds')
        if tcp_interval not in (0, .5, 1., 2., 5.):
            raise ValueError('tcp_interval must be 0, 0.5, 1, 2, or 5 seconds')
        if kv_cache_memory_bytes is not None and kv_cache_memory_bytes <= 0:
            raise ValueError('KV cache budget must be positive')
        result = cls(profile, ctx, ctx, limit, float(metrics_interval), bool(detailed_events),
                     float(tcp_interval), kv_cache_memory_bytes)
        result.validate()
        return result

    def validate(self):
        if self.codex_context != self.server_context:
            raise ValueError('Context window inconsistent across Codex/vLLM')
        if not 0 < self.auto_compact_threshold <= self.codex_context - 1024:
            raise ValueError('Compact threshold must leave >=1024 output tokens')
        if self.server_context not in (v[0] for v in PROFILES.values()):
            raise ValueError('Unsupported context config')

    def as_dict(self):
        return {**dataclasses.asdict(self), 'stable_vllm': dict(STABLE),
                'deployment_verified': False}

    def update_catalog(self, source: pathlib.Path, dest: pathlib.Path):
        self.validate()
        catalog = json.loads(source.read_text())
        entry = next((m for m in catalog['models'] if m['slug'] == MODEL), None)
        if entry is None:
            raise ValueError('Missing exact model Catalog entry')
        entry['context_window'] = self.codex_context
        entry['max_context_window'] = self.codex_context
        dest.write_text(json.dumps(catalog, indent=2, ensure_ascii=False)+'\n')

    def update_codex(self, text: str, base_url: str) -> str:
        import re
        for key, value in [('model_context_window', self.codex_context),
                           ('model_auto_compact_token_limit', self.auto_compact_threshold)]:
            text, count = re.subn(rf'(?m)^{key}\s*=\s*\d+\s*$', f'{key} = {value}', text)
            if count != 1:
                raise ValueError(f'Missing or duplicate config key {key}')
        text = text.replace('http://127.0.0.1:18000/v1', base_url)
        return text


def extract_vllm_args(args):
    """Inspect docker Config.Cmd/Args (actual process config, not a catalog claim)."""
    found = {}
    for flag, field in [('--max-model-len', 'server_context'),
                        ('--gpu-memory-utilization', 'gpu_memory_utilization'),
                        ('--max-num-batched-tokens', 'max_num_batched_tokens'),
                        ('--max-num-seqs', 'max_num_seqs'),
                        ('--dtype', 'dtype'), ('--tensor-parallel-size', 'tensor_parallel_size'),
                        ('--kv-cache-memory-bytes', 'kv_cache_memory_bytes')]:
        try: found[field] = args[args.index(flag)+1]
        except (ValueError,IndexError): continue
    found['enforce_eager'] = '--enforce-eager' in args
    found['prefix_caching'] = '--enable-prefix-caching' in args
    found['detailed_events'] = any(a == 'agent_traffic_v3.AgentTrafficMiddleware' for a in args)
    return found


def assert_deployment(actual: dict, target: ExperimentConfig, *, need_events=True):
    expected = {'server_context': target.server_context, **STABLE}
    if target.kv_cache_memory_bytes is not None:
        expected['kv_cache_memory_bytes'] = target.kv_cache_memory_bytes
    if need_events:
        expected['detailed_events'] = target.detailed_events
    issues=[]
    for key, value in expected.items():
        real=actual.get(key)
        if key in ('server_context','max_num_batched_tokens','max_num_seqs','tensor_parallel_size','kv_cache_memory_bytes'):
            try: real=int(real)
            except (ValueError,TypeError): pass
        elif key=='gpu_memory_utilization':
            try: real=float(real)
            except (ValueError,TypeError): pass
        if real != value: issues.append(f'{key}: actual={real!r}, expected={value!r}')
    if issues: raise RuntimeError('Model deployment config mismatch: '+ '; '.join(issues))
    return True
