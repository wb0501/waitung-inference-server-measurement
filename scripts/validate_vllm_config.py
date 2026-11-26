#!/usr/bin/env python3
"""Static constructors and mocked compile calls only; never instantiate/load a model."""
import json,os,pathlib,sys,tempfile,unittest.mock
import torch,vllm
from vllm.entrypoints.openai.cli_args import make_arg_parser,validate_parsed_serve_args
from vllm.utils.argparse_utils import FlexibleArgumentParser
from vllm.engine.arg_utils import AsyncEngineArgs
parser=make_arg_parser(FlexibleArgumentParser());args=parser.parse_args(sys.argv[1:]);validate_parsed_serve_args(args)
engine=AsyncEngineArgs.from_cli_args(args)
assert vllm.__version__=='0.24.0'
assert engine.gpu_memory_utilization==0.70 and engine.max_model_len in (8192,16384,32768,65536)
assert args.max_model_len==engine.max_model_len
assert engine.max_num_batched_tokens==1024 and engine.max_num_seqs==1
assert engine.tensor_parallel_size==1 and engine.dtype=='bfloat16' and engine.language_model_only
assert args.reasoning_parser=='qwen3' and args.tool_call_parser=='qwen3_coder' and args.enable_auto_tool_choice
from vllm.config import KernelConfig
kc=engine.kernel_config
if isinstance(kc,dict):kc=KernelConfig(**kc)
assert kc.moe_backend=='triton' and not kc.enable_flashinfer_autotune
from vllm.config import SchedulerConfig
scheduler=SchedulerConfig(is_encoder_decoder=False,max_model_len=engine.max_model_len,max_num_batched_tokens=1024,max_num_seqs=1,enable_chunked_prefill=True)
assert scheduler.max_num_batched_tokens==1024 and scheduler.max_num_seqs==1
from flashinfer.jit import cpp_ext
assert cpp_ext._get_num_workers()==2
assert cpp_ext.get_nvcc_parallelism_flags()==['--threads=1']
with tempfile.TemporaryDirectory() as d:
 with unittest.mock.patch.object(cpp_ext.subprocess,'run') as call:
  cpp_ext.run_ninja(pathlib.Path(d),pathlib.Path(d)/'test.ninja',False)
  command=call.call_args.args[0]
  assert command[-2:]==['-j','2'],command
from vllm.model_executor.layers.fused_moe.config import FusedMoEConfig,FusedMoEParallelConfig
from vllm.model_executor.layers.fused_moe.activation import MoEActivation
from vllm.model_executor.layers.fused_moe.oracle.unquantized import select_unquantized_moe_backend
from vllm.model_executor.layers.fused_moe.config import RoutingMethodType
c=json.loads(pathlib.Path('/model/config.json').read_text());t=c['text_config']
assert c['architectures']==['Qwen3_5MoeForConditionalGeneration']
# Qwen3NextSparseMoeBlock: BF16, SILU, renormalized top-k routing, TP/EP/DP=1.
print('Routing enum:',list(RoutingMethodType),flush=True)
routing=RoutingMethodType.RenormalizeNaive
moe=FusedMoEConfig(num_experts=t['num_experts'],experts_per_token=t['num_experts_per_tok'],hidden_dim=t['hidden_size'],intermediate_size=t['moe_intermediate_size'],num_local_experts=t['num_experts'],num_logical_experts=t['num_experts'],activation=MoEActivation.SILU,device='cuda',routing_method=routing,moe_parallel_config=FusedMoEParallelConfig.make_no_parallel(),in_dtype=torch.bfloat16,moe_backend='triton',max_num_tokens=1024)
backend,cls=select_unquantized_moe_backend(moe)
assert backend.value=='TRITON'
result={'status':'passed','validation_scope':'CLI/config constructors and backend support predicates; no engine, model weights, CUDA kernels or compiler launched','vllm':vllm.__version__,'torch':torch.__version__,'cuda':torch.version.cuda,'gpu':torch.cuda.get_device_name(),'compiler_command_mock':command,'flashinfer_nvcc_flags':cpp_ext.get_nvcc_parallelism_flags(),'triton_backend':backend.value,'kernel_class':cls.__name__,'gpu_memory_utilization':engine.gpu_memory_utilization,'max_model_len':engine.max_model_len,'max_num_batched_tokens':engine.max_num_batched_tokens,'max_num_seqs':engine.max_num_seqs}
print(json.dumps(result,indent=2),flush=True)
