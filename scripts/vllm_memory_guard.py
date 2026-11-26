#!/usr/bin/env python3
"""Independent host-memory guard. Docker socket access is restricted by exact ID + labels in code."""
import argparse,datetime,http.client,json,os,pathlib,socket,subprocess,time,traceback
GIB=1024**3
p=argparse.ArgumentParser();p.add_argument('--target-id',required=True);p.add_argument('--run-id',required=True);p.add_argument('--test-scenario',choices=['low-memory','pressure']);a=p.parse_args()
ROOT=pathlib.Path('/guard');ROOT.mkdir(exist_ok=True)
class Docker(http.client.HTTPConnection):
 def __init__(self):super().__init__('localhost',timeout=3)
 def connect(self):self.sock=socket.socket(socket.AF_UNIX);self.sock.settimeout(self.timeout);self.sock.connect('/var/run/docker.sock')
def api(path,method='GET',raw=False):
 c=Docker()
 try:
  c.request(method,path);r=c.getresponse();b=r.read(8*1024*1024)
  if not 200<=r.status<300:raise RuntimeError(f'Docker {method} {path}: {r.status} {b[:300]!r}')
  return b if raw else (json.loads(b) if b else None)
 finally:c.close()
def inspect():
 d=api('/containers/'+a.target_id+'/json');labels=d['Config'].get('Labels') or {}
 if d['Id']!=a.target_id or labels.get('owner')!='waitung-qwen36':raise RuntimeError('Target ID/owner mismatch')
 if a.test_scenario and labels.get('role')!='safety-test':raise RuntimeError('Synthetic test forbidden for model containers')
 return d
def write_status(state,**extra):
 v={'state':state,'run_id':a.run_id,'target_id':a.target_id,'monotonic':time.monotonic(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),**extra}
 tmp=ROOT/'status.tmp';tmp.write_text(json.dumps(v));tmp.replace(ROOT/'status.json');return v
def log(v):
 print(json.dumps(v),flush=True)
 try:
  with (ROOT/'metrics.jsonl').open('a') as f:f.write(json.dumps(v)+'\n')
 except OSError as e:print(json.dumps({'event':'metrics_write_error','error':str(e)}),flush=True)
def text(path):return pathlib.Path(path).read_text().strip()
def psi(kind):
 result={}
 for line in text('/host/proc/pressure/'+kind).splitlines():
  parts=line.split();result[parts[0]]={k:float(v) for k,v in (x.split('=') for x in parts[1:])}
 return result
def counters(path):return {k:int(v) for k,v in (x.split() for x in text(path).splitlines())}
def host_sample():
 m={k:int(v.split()[0])*1024 for k,v in (line.split(':',1) for line in text('/host/proc/meminfo').splitlines())}
 return {'mem_available_bytes':m['MemAvailable'],'swap_used_bytes':m['SwapTotal']-m['SwapFree'],'swap_total_bytes':m['SwapTotal'],'memory_psi':psi('memory'),'cpu_psi':psi('cpu'),'loadavg':text('/host/proc/loadavg'),'root_memory_events':counters('/host/cgroup/memory.events') if pathlib.Path('/host/cgroup/memory.events').exists() else None}
def cgroup_sample(pid):
 lines=text(f'/host/proc/{pid}/cgroup').splitlines();rel=next(l.split(':',2)[2] for l in lines if l.startswith('0::'))
 root=pathlib.Path('/host/cgroup').resolve();path=(root/rel.lstrip('/')).resolve()
 if not path.is_relative_to(root):raise RuntimeError('Invalid cgroup path')
 result={'path':str(path),'memory_events':counters(path/'memory.events')}
 for key in ['memory.current','memory.peak','memory.max','memory.swap.current','memory.swap.max','memory.pressure']:
  try:result[key]=text(path/key)
  except FileNotFoundError:pass
 return result
def save_logs():
 try:
  raw=api('/containers/'+a.target_id+'/logs?stdout=1&stderr=1&timestamps=1&tail=2000',raw=True)
  # Docker non-TTY logs use 8-byte multiplex headers.
  chunks=[];i=0
  while i+8<=len(raw):
   size=int.from_bytes(raw[i+4:i+8],'big');chunks.append(raw[i+8:i+8+size]);i+=8+size
  data=b''.join(chunks) if chunks else raw
  tmp=ROOT/'vllm.tmp';tmp.write_bytes(data);tmp.replace(ROOT/'vllm.log')
 except Exception as e:log({'event':'log_snapshot_error','error':str(e)})
def stop(reason,sample=None):
 try:write_status('triggered',reason=reason)
 except Exception as e:log({'event':'status_write_error','error':str(e)})
 log({'event':'stop_trigger','reason':reason,'sample':sample,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
 # Revalidate ownership immediately before every mutation. Never address a name.
 for method,path in [('POST',f'/containers/{a.target_id}/stop?t=3'),('POST',f'/containers/{a.target_id}/kill')]:
  try:
   d=inspect()
   if not d['State']['Running']:break
   api(path,method)
  except Exception as e:log({'event':'stop_error','error':str(e)})
 try:
  d=inspect();log({'event':'stop_result','running':d['State']['Running'],'exit_code':d['State']['ExitCode']})
 except Exception as e:log({'event':'stop_verification_error','error':str(e)})
 save_logs()
started=time.monotonic();pressure_since=None;errors=0;seen_running=False;last_gpu=-100;last_logs=-100;baseline_events=None
try:
 inspect() # No synthetic mode or monitoring is allowed without validating target identity.
 while True:
  cycle=time.monotonic()
  try:
   d=inspect();s=host_sample();s.update({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'monotonic':cycle,'container_state':d['State']['Status'],'synthetic':bool(a.test_scenario)})
   if d['State']['Running']:
    seen_running=True;s['container_cgroup']=cgroup_sample(d['State']['Pid']);ev=s['container_cgroup']['memory_events']
    if baseline_events is None:baseline_events={k:0 for k in ev}
   else:ev={}
   if cycle-last_gpu>=10:
    last_gpu=cycle
    try:
     q=subprocess.run(['nvidia-smi','--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu','--format=csv,noheader,nounits'],capture_output=True,text=True,timeout=3)
     s['gpu']={'returncode':q.returncode,'stdout':q.stdout.strip(),'stderr':q.stderr.strip()}
    except Exception as e:s['gpu']={'error':str(e)}
   if a.test_scenario=='low-memory':s['mem_available_bytes']=19*GIB
   if a.test_scenario=='pressure':s['memory_psi']['full']['avg10']=25.0
   log(s);errors=0
   reason=None
   if s['mem_available_bytes']<20*GIB:reason='MemAvailable below 20 GiB'
   severe=s['memory_psi'].get('full',{}).get('avg10',0)>=20 or s['memory_psi'].get('some',{}).get('avg10',0)>=60
   pressure_since=(pressure_since if pressure_since is not None else cycle) if severe else None
   duration=15
   if pressure_since is not None and cycle-pressure_since>=duration:reason=f'Sustained severe memory PSI for {duration}s (full avg10 >=20% or some >=60%)'
   if ev and any(ev.get(k,0)>baseline_events.get(k,0) for k in ['oom','oom_kill','oom_group_kill']):reason='Container cgroup OOM event'
   if reason:stop(reason,s);break
   if seen_running and not d['State']['Running']:
    save_logs();write_status('finished',exit_code=d['State']['ExitCode']);break
   if not seen_running and cycle-started>120:stop('Target never started within 120s');break
   write_status('armed',mem_available_bytes=s['mem_available_bytes'],memory_psi=s['memory_psi'])
   if cycle-last_logs>=10:last_logs=cycle;save_logs()
  except Exception as e:
   errors+=1;log({'event':'sample_error','consecutive':errors,'error':str(e)})
   # Do not refresh heartbeat on errors: the container supervisor fails closed.
   if errors>=3:stop('Three consecutive guard sampling failures');break
  time.sleep(max(0.1,2-(time.monotonic()-cycle)))
except BaseException as e:
 log({'event':'guard_fatal','error':repr(e),'traceback':traceback.format_exc()})
 try:stop('Guard fatal exception')
 except Exception:pass
 raise
