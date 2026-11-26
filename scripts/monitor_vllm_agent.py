import time,pathlib,traceback
from validate_vllm_live import Session
s=Session();s.stage='agent_monitor';end=time.monotonic()+1800
try:
 while time.monotonic()<end and not (s.root/'agent-monitor.done').exists():
  s.snapshot();time.sleep(1)
except BaseException as e:
 (s.root/'agent-monitor.error').write_text(traceback.format_exc());s.stop(str(e))
finally:
 (s.root/'agent-monitor.finished').write_text(str(time.time()))
