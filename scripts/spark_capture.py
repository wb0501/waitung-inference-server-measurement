#!/usr/bin/env python3
"""Owned per-task Spark capture. No model or system configuration mutations."""
import argparse, datetime, fcntl, hashlib, json, os, pathlib, re, signal, subprocess, sys, time, urllib.request, threading, math
B=pathlib.Path('/home/huanglabspk/WaiTung'); ROOT=B/'logs/agent-capture'
NAME='waitung-vllm-qwen36'; WATCH='waitung-vllm-watchdog'; OWNER='waitung-qwen36'
# Confirmed in vLLM v0.24.0 vllm/v1/metrics/loggers.py. Metric names
# on the wire may have Prometheus counter suffix _total or histogram suffixes.
# Runtime metric family definitions come from the actual /metrics TYPE records.
# vLLM 0.24.0's exported names are not assumed to be exhaustive.
METRIC_LINE=re.compile(r'^([^{}\s]+)(\{[^}]*\})?\s+([-+0-9.eE]+|Inf|-Inf|NaN)(?:\s+.*)?$')
def parse_prometheus_sample(body):
 types={};helps={};observed=[];metrics=[]
 for line in body.splitlines():
  if line.startswith('# TYPE '):
   parts=line.split(None,3)
   if len(parts)==4:types[parts[2]]=parts[3]
  elif line.startswith('# HELP '):
   parts=line.split(None,3)
   if len(parts)==4:helps[parts[2]]=parts[3]
 for line in body.splitlines():
  match=METRIC_LINE.match(line)
  if not match:continue
  name,labels,value=match.groups()
  # Preserve all exposed metric families, including process/runtime statistics.
  family=name;component=None
  if family not in types:
   for suffix in ('_bucket','_count','_sum','_total','_created'):
    if name.endswith(suffix) and name[:-len(suffix)] in types:
     family=name[:-len(suffix)];component=suffix[1:];break
  kind=types.get(family,'untyped')
  try:floatvalue=float(value)
  except ValueError:continue
  observed.append(name)
  metrics.append({'name':name,'family':family,'kind':kind,'component':component,
                  'labels':labels or '', 'value':floatvalue if math.isfinite(floatvalue) else value})
 return metrics,sorted(set(observed)),{'types':types,'help':helps}

_LAST_METRIC_COUNTERS={}
_LAST_METRIC_PROCESS_START=None

def sample_metrics(p,interval):
 global _LAST_METRIC_PROCESS_START
 started=now();started_mono=time.monotonic_ns();record={'scrape_start_ns':started,'interval_seconds':interval,
                        'endpoint':'http://127.0.0.1:8000/metrics',
                        'measurement_control':True,'sample_clock':'CLOCK_REALTIME + CLOCK_MONOTONIC','sample_start_monotonic_ns':started_mono}
 try:
  with urllib.request.urlopen('http://127.0.0.1:8000/metrics',timeout=.75) as response:
   if response.status != 200:raise RuntimeError('metrics HTTP '+str(response.status))
   raw=response.read(2_000_001)
   if len(raw)>2_000_000:raise RuntimeError('metrics response exceeds bound; sample incomplete')
   body=raw.decode('utf-8')
  record['metrics'],record['metric_names'],record['families']=parse_prometheus_sample(body)
  process=next((line.split()[-1] for line in body.splitlines()
                if line.startswith('process_start_time_seconds ')),None)
  record['process_start_time_seconds']=float(process) if process is not None else None
  record['service_process_changed']=(_LAST_METRIC_PROCESS_START is not None and
       record['process_start_time_seconds']!=_LAST_METRIC_PROCESS_START)
  if record['service_process_changed']:_LAST_METRIC_COUNTERS.clear()
  _LAST_METRIC_PROCESS_START=record['process_start_time_seconds']
  resets=[]
  for metric in record['metrics']:
   if not (metric['kind']=='counter' or metric['kind']=='histogram' and metric['component'] in ('count','bucket','sum')) or metric['name'].endswith('_created') or not isinstance(metric['value'],(int,float)):continue
   key=(metric['name'],metric['labels'])
   previous=_LAST_METRIC_COUNTERS.get(key)
   if previous is not None and metric['value']<previous:resets.append({'name':metric['name'],'labels':metric['labels'],'before':previous,'after':metric['value']})
   _LAST_METRIC_COUNTERS[key]=metric['value']
  record['counter_resets']=resets
  record['raw_text']=body
  record['raw_bytes']=len(body.encode('utf-8'))
  record['raw_sha256']=hashlib.sha256(body.encode('utf-8')).hexdigest()
  record['scrape_duration_ns']=time.monotonic_ns()-started_mono
 except Exception as e:
  record.update(error=repr(e),metrics=[],metric_names=[],families={'types':{},'help':{}})
 record['observed_ns']=now();record['sample_end_monotonic_ns']=time.monotonic_ns();record['scrape_duration_ns']=record['sample_end_monotonic_ns']-started_mono
 with (p/'metrics_samples.jsonl').open('a') as f:f.write(json.dumps(record)+'\n')
def file_sha256(path):
 with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def now(): return time.time_ns()
def write(p,j):
 q=p.with_suffix('.tmp');q.write_text(json.dumps(j,indent=2));q.replace(p)
def inspect(n):return json.loads(subprocess.check_output(['docker','inspect',n],timeout=8))[0]
def model(check_http=True):
 """Report actual owned Docker configuration, tolerating never-created containers."""
 def inspect_optional(name):
  try:return inspect(name)
  except subprocess.CalledProcessError:return None
 m=inspect_optional(NAME);w=inspect_optional(WATCH)
 for container in (m,w):
  if container and container['Config']['Labels'].get('owner')!=OWNER:
   raise RuntimeError('Foreign model/watchdog ownership; refusing action')
 mem={k:int(v.split()[0])*1024 for k,v in (line.split(':',1) for line in pathlib.Path('/proc/meminfo').read_text().splitlines())}
 from experiment_config import extract_vllm_args
 actual=extract_vllm_args(m['Config'].get('Cmd') or []) if m else {}
 model_state=m['State'] if m else {'Running':False,'OOMKilled':False}
 watchdog_state=w['State'] if w else {'Running':False}
 result={'runtime_config':actual,'model_state':model_state,'watchdog_state':watchdog_state,
         'container_id':m['Id'] if m else None,'mem_available_bytes':mem['MemAvailable'],
         'swap_used_bytes':mem['SwapTotal']-mem['SwapFree'],
         'memory_psi':pathlib.Path('/proc/pressure/memory').read_text(),'health':None}
 if m and model_state['Running']:
  guard=pathlib.Path(next(x['Source'] for x in m['Mounts'] if x['Destination']=='/guard'))
  hb=json.loads((guard/'status.json').read_text())
  result['guard']=hb;result['guard_age_seconds']=time.monotonic()-hb['monotonic']
  rel=next(line.split(':',2)[2] for line in pathlib.Path(f"/proc/{model_state['Pid']}/cgroup").read_text().splitlines() if line.startswith('0::'))
  cg=pathlib.Path('/sys/fs/cgroup')/rel.lstrip('/')
  result['memory_events']={k:int(v) for k,v in (line.split() for line in (cg/'memory.events').read_text().splitlines())}
  result['limits']={k:(cg/k).read_text().strip() for k in ['memory.max','memory.swap.max']}
  if check_http:
   try:
    with urllib.request.urlopen('http://127.0.0.1:8000/health',timeout=3) as r:result['health']=r.status
   except Exception as exc:result['health_error']=repr(exc)
 result['safe']=bool(model_state['Running'] and not model_state['OOMKilled'] and watchdog_state['Running']
      and result.get('guard',{}).get('state')=='armed' and result.get('guard_age_seconds',999)<8
      and mem['MemAvailable']>25*1024**3
      and not any(result.get('memory_events',{}).get(key,0) for key in ('oom','oom_kill')))
 result['generation_proof']=None
 if m:
  for file in (B/'deployment/model-readiness.json', B/'deployment/validation.json'):
   try:
    proof=json.loads(file.read_text());proof=proof.get('live_api_validation',proof)
    if proof.get('container_id',proof.get('model_container_id'))==m['Id'] and (
        proof.get('status')=='passed' or (proof.get('tests',{}).get('responses',{}).get('status')=='passed'
        and proof.get('tests',{}).get('cold_chat',{}).get('status')=='passed')):
     result['generation_proof']={'source':str(file),'container_id':m['Id'],'responses_generation_verified':True}
   except (OSError,ValueError):pass
 return result
def session(run,task):
 for value in [run,task]:
  if value in ('.','..') or not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',value):raise ValueError('Invalid session ID')
 return ROOT/run/task
def pid_alive(pid):
 try:os.kill(pid,0);return True
 except ProcessLookupError:return False
def terminate_capture(p):
 path=p/'state.json'
 if not path.exists():return
 s=json.loads(path.read_text());pid=s.get('dumpcap_pid')
 if pid and pathlib.Path(f'/proc/{pid}/cmdline').exists():
  cmd=pathlib.Path(f'/proc/{pid}/cmdline').read_bytes()
  if b'dumpcap' in cmd and str(p/'server_traffic.pcapng').encode() in cmd:os.kill(pid,signal.SIGINT)
def parse_ss_output(raw):
 """Parse both ss -tinmp with and without the optional state column.

 Connection attribution remains unknown unless the caller can prove the SSH
 tunnel identity. Full raw output is retained with the sampled record.
 """
 sockets=[];current=None
 for line in raw.splitlines():
  stripped=line.strip()
  if not stripped:continue
  parts=stripped.split()
  # ssh transport line: optional ESTAB, then Recv-Q Send-Q Local Peer.
  offset=1 if parts[0] in ('ESTAB','ESTABLISHED') else 0
  if len(parts)>=offset+4 and all(part.isdigit() for part in parts[offset:offset+2]) and      ':' in parts[offset+2] and ':' in parts[offset+3]:
   local,remote=parts[offset+2:offset+4]
   current={'local':local,'remote':remote,'socket_identity':local+'->'+remote,
            'layer':'physical_ssh_transport','connection_attribution':'unknown',
            'source':'ss -tinmp','recv_queue_bytes':int(parts[offset]),'send_queue_bytes':int(parts[offset+1]),'transport':'tcp','state':'ESTABLISHED'}
   sockets.append(current)
   # The first row may also contain socket details after the endpoints.
   stripped=' '.join(parts[offset+4:])
  if current and stripped:
   keys=('rtt','minrtt','cwnd','retrans','unacked','notsent','notsent_bytes',
         'pacing_rate','delivery_rate','rcv_space','rto','bytes_acked',
         'bytes_sent','segs_out','segs_in','send','rcv_ssthresh','skmem','bytes_retrans','bytes_received','snd_wnd','delivered','rcv_rtt')
   pattern=r'(?<!\w)('+ '|'.join(keys)+r')(?::|\s+)(\S+)'
   for key,value in re.findall(pattern,stripped):current[key]=value
 return sockets

def sample_tcp(p,interval,peer):
 """Low-rate host SSH TCP snapshots. Never infer SSE ownership from this alone."""
 t0=now();m0=time.monotonic_ns()
 try:
  proc=subprocess.run(['ss','-tinmp','state','established','(','sport','=',':22',')'],
       capture_output=True,text=True,timeout=.65)
  raw=proc.stdout[:120_000] if proc.returncode==0 else ''
  error=(proc.stderr[:2000] or f'rc={proc.returncode}') if proc.returncode else None
 except Exception as exc:raw='';error=repr(exc)
 record={'sample_start_ns':t0,'observed_ns':now(),'sample_duration_ns':time.monotonic_ns()-m0,
         'sample_clock':'CLOCK_REALTIME + CLOCK_MONOTONIC','sample_start_monotonic_ns':m0,'sample_end_monotonic_ns':time.monotonic_ns(),'network_namespace':os.readlink('/proc/self/ns/net'),'role':'physical_ssh_host_socket',
         'query':'ss -tinmp state established ( sport = :22 )','peer_ip':peer,
         'connections':parse_ss_output(raw),'raw_output':raw,'truncated':len(raw)>=120_000,'error':error,
         'warning':'May include multiple SSH connections. Not a matched Agent flow; no per-SSE attribution.',
         'measurement_control':True,'interval_seconds':interval}
 # Only the observed control connection's full tuple can be positively attributed.
 control=json.loads((p/'state.json').read_text()).get('ssh_capture_control_connection',{})
 for c in record['connections']:
  c['network_namespace']=record['network_namespace']
  if c['local']==f"{control.get('server_ip')}:{control.get('server_port')}" and c['remote']==f"{control.get('client_ip')}:{control.get('client_port')}":c['connection_attribution']='ssh_capture_control'
 with (p/'tcp_samples.jsonl').open('a') as f:f.write(json.dumps(record)+'\n')

def collect_telemetry_window(p,state):
 """Non-destructively extract service-wide event log subset by capture clock bounds."""
 start=state.get('ready_ns',state.get('started_ns',0));end=state.get('ended_ns',now())
 if not start:return
 directory=B/'runtime/v3_telemetry'
 count=0;invalid_lines=0
 with (p/'v3_events.jsonl').open('w') as dest:
  for source in sorted(directory.glob('events-*.jsonl')):
   with source.open(errors='replace') as f:
    for line in f:
     try:j=json.loads(line)
     except ValueError:invalid_lines+=1;continue
     if start<=j.get('timestamp_ns',0)<=end:
      j['observed_from_file']=source.name
      dest.write(json.dumps(j,ensure_ascii=False)+'\n');count+=1
 snapshots=[]
 for source in sorted(directory.glob('writer-*.json')):
  try:snapshots.append(json.loads(source.read_text()))
  except (OSError,ValueError):snapshots.append({'file':source.name,'error':'unreadable'})
 (p/'v3_telemetry_collection.json').write_text(json.dumps({'events':count,'start_ns':start,'end_ns':end,
     'writer_stats':snapshots,'snapshot_observed_ns':now(),'invalid_source_lines':invalid_lines,
     'writer_stats_completeness':'periodic snapshots; final zero-loss cannot be established while vLLM is running; snapshot file mtimes are not event barriers',
     'writer_snapshot_files':[{'name':x.name,'mtime_ns':x.stat().st_mtime_ns} for x in sorted(directory.glob('writer-*.json'))],
     'source_files':[{'name':x.name,'bytes':x.stat().st_size} for x in sorted(directory.glob('events-*.jsonl'))],
     'note':'Raw service logs retained; no synthetic zero-loss claim.'},indent=2))

def periodic_sample(interval,callback,stop,stats):
 """Schedule by monotonic deadlines; skip expired periods, no catch-up burst."""
 deadline=time.monotonic();stats['started_ns']=now()
 try:
  while not stop.is_set():
   if stop.wait(max(0,deadline-time.monotonic())):break
   stats['attempts']+=1
   try:callback()
   except Exception as exc:stats['errors'].append(repr(exc))
   deadline+=interval
   current=time.monotonic()
   if current>deadline:
    missed=math.floor((current-deadline)/interval)+1
    stats['missed_cycles']+=missed;deadline+=missed*interval
 finally:stats['ended_ns']=now()

def worker(p,max_seconds,metrics_interval=1.,tcp_interval=1.,detailed_events=1):
 lock=(ROOT/'active.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 interrupted=[False]
 for sig in [signal.SIGTERM,signal.SIGINT]:signal.signal(sig,lambda *_:interrupted.__setitem__(0,True))
 s=json.loads((p/'state.json').read_text());s['worker_pid']=os.getpid();s['started_ns']=now()
 try:
  m=inspect(NAME);net=next(iter(m['NetworkSettings']['Networks'].values()));ip=net['IPAddress'];peer=s['client_ip']
  physical=json.loads(subprocess.check_output(['ip','-j','route','get',peer],text=True))[0]['dev']
  bridge=json.loads(subprocess.check_output(['docker','network','inspect',net['NetworkID']],text=True))[0];bridge_name=bridge.get('Options',{}).get('com.docker.network.bridge.name') or ('docker0' if bridge['Name']=='bridge' else 'br-'+bridge['Id'][:12])
  links=json.loads(subprocess.check_output(['ip','-j','link'],text=True));veth=[]
  for entry in pathlib.Path('/sys/class/net').iterdir():
   master=entry/'master'
   if master.exists() and master.resolve().name==bridge_name and entry.name.startswith('veth'):veth.append(entry.name)
  # All bridge veths are recorded and filtered to this container IP/8000.
  interfaces=[
   {'name':physical,'point':'physical_ssh','filter':f'host {peer} and tcp port 22', 'network_layer':'physical SSH/TCP','traffic_roles':['ssh_tunnel','ssh_capture_control','scp_collection'],'byte_accounting':'excluded_from_http'},
   {'name':'lo','point':'host_loopback_http','filter':'tcp port 8000','network_layer':'loopback HTTP/SSE','traffic_roles':['model_http','health_control','metrics_control'],'byte_accounting':'duplicate_view_excluded'},
   {'name':bridge_name,'point':'container_bridge_http','filter':f'host {ip} and tcp port 8000','network_layer':'Docker bridge TCP','traffic_roles':['model_http','health_control','metrics_control'],'byte_accounting':'canonical_http_view'}
  ]+[{'name':x,'point':'bridge_veth_http','filter':f'host {ip} and tcp port 8000','network_layer':'Docker veth TCP','traffic_roles':['model_http','health_control','metrics_control'],'byte_accounting':'duplicate_view_excluded'} for x in veth]
  cmd=['dumpcap','-q','-B','8','-a',f'duration:{max_seconds}','-a','filesize:1048576']
  for i in interfaces:cmd+=['-i',i['name'],'-f',i['filter']]
  cmd+=['-w',str(p/'server_traffic.pcapng')]
  with (p/'capture.log').open('wb') as log:proc=subprocess.Popen(cmd,stdout=log,stderr=log,start_new_session=True)
  s.update(dumpcap_pid=proc.pid,interfaces=interfaces,analysis_interface=bridge_name,container_ip=ip,command=cmd,
           metrics_interval_seconds=metrics_interval,
           metrics_endpoint='http://127.0.0.1:8000/metrics' if metrics_interval else None,
           aggregation_policy='Canonical bridge HTTP bytes; never sum interfaces');write(p/'state.json',s)
  for _ in range(100):
   if proc.poll() is not None:raise RuntimeError('dumpcap failed before READY')
   if (p/'server_traffic.pcapng').exists() and (p/'server_traffic.pcapng').stat().st_size>=28 and "File:" in (p/'capture.log').read_text(errors='replace'):break
   time.sleep(.1)
  else:raise RuntimeError('No capture READY within 10s')
  s.update(state='READY',ready_ns=now(),heartbeat_ns=now());write(p/'state.json',s)
  samplers=[];sampling_stop=threading.Event();sampling_stats={}
  for label,interval,callback in [('prometheus',metrics_interval,lambda:sample_metrics(p,metrics_interval)),('tcp',tcp_interval,lambda:sample_tcp(p,tcp_interval,peer))]:
   if interval:
    stats={'missed_cycles':0,'attempts':0,'errors':[]};sampling_stats[label]=stats
    thread=threading.Thread(target=periodic_sample,args=(interval,callback,sampling_stop,stats),daemon=True,name='capture-'+label)
    samplers.append(thread);thread.start()
  next_safety=0
  while proc.poll() is None:
   if interrupted[0] or (p/'stop').exists():break
   if time.monotonic()>=next_safety:
    if time.time()-(p/'lease').stat().st_mtime>45:s['stop_reason']='controller lease expired';break
    if not model(False)['safe']:s['stop_reason']='model/watchdog resource safety failed';break
    s['heartbeat_ns']=now();write(p/'state.json',s);next_safety=time.monotonic()+1
   time.sleep(.1)
  sampling_stop.set()
  for thread in samplers:thread.join(timeout=5)
  s['sampling_missed_cycles']={k:v['missed_cycles'] for k,v in sampling_stats.items()}
  s['sampling_worker_stats']=sampling_stats
  if any(thread.is_alive() for thread in samplers):raise RuntimeError('Sampling shutdown not confirmed')
  if proc.poll() is None:proc.send_signal(signal.SIGINT)
  try:proc.wait(timeout=15)
  except subprocess.TimeoutExpired:proc.kill();proc.wait();s['stop_reason']='forced capture termination'
  s.update(state='STOPPED',ended_ns=now(),dumpcap_exit_code=proc.returncode)
 except BaseException as e:
  terminate_capture(p);s.update(state='FAILED',error=repr(e),ended_ns=now())
 finally:
  if 'sampling_stop' in locals():
   sampling_stop.set()
   for thread in samplers:thread.join(timeout=5)
  if detailed_events:
   try:collect_telemetry_window(p,s)
   except Exception as exc:
    s['telemetry_collection_error']=repr(exc)
    (p/'telemetry_collection_error.txt').write_text(repr(exc))
  write(p/'state.json',s)
def clock():return {'realtime_ns':now(),'monotonic_ns':time.monotonic_ns(),'clock_source':pathlib.Path('/sys/devices/system/clocksource/clocksource0/current_clocksource').read_text().strip(),'method':'CLOCK_REALTIME via time.time_ns; monotonic for durations'}
def main():
 ap=argparse.ArgumentParser();ap.add_argument('action',choices=['start','status','stop','collect','worker','model','clock','generate','launch','launch-worker','launch-status']);ap.add_argument('--run-id');ap.add_argument('--task-id');ap.add_argument('--max-seconds',type=int,default=3900);ap.add_argument('--path');ap.add_argument('--metrics-interval',type=float,default=1.0);ap.add_argument('--tcp-interval',type=float,default=1.0);ap.add_argument('--detailed-events',type=int,default=1);ap.add_argument('--context-window',type=int,default=8192);ap.add_argument('--kv-cache-memory-bytes',type=int); ap.add_argument('--tunnel-local-port',type=int);ap.add_argument('--tunnel-pid',type=int);args=ap.parse_args()
 if args.action=='model':print(json.dumps(model()));return
 if args.action=='clock':print(json.dumps(clock()));return
 if args.action=='generate':
  req=urllib.request.Request('http://127.0.0.1:8000/v1/responses',data=json.dumps({'model':'Qwen3.6-35B-A3B','input':'Reply OK.','max_output_tokens':16,'store':False,'temperature':0,'chat_template_kwargs':{'enable_thinking':False}}).encode(),headers={'Content-Type':'application/json'})
  start=time.monotonic()
  with urllib.request.urlopen(req,timeout=180) as r:j=json.load(r)
  if j.get('status')!='completed' or not j.get('output'):raise RuntimeError(str(j))
  proof={'status':'passed','container_id':inspect(NAME)['Id'],'elapsed_seconds':time.monotonic()-start,'checked_ns':now(),'response':j};write(B/'deployment/model-readiness.json',proof);print(json.dumps(proof));return
 ROOT.mkdir(parents=True,exist_ok=True)
 if args.action=='worker':worker(pathlib.Path(args.path),args.max_seconds,args.metrics_interval,args.tcp_interval,args.detailed_events);return
 p=session(args.run_id,args.task_id)
 if args.action=='launch':
  m=model()
  if m['model_state']['Running']:raise RuntimeError('Refusing launch of running model')
  if m['mem_available_bytes']<110*1024**3:raise RuntimeError('Need 110GiB before existing startup script')
  p.mkdir(parents=True,exist_ok=False)
  with (p/'launch.log').open('wb') as f:proc=subprocess.Popen([sys.executable,__file__,'launch-worker','--run-id',args.run_id,'--task-id',args.task_id,'--context-window',str(args.context_window),'--detailed-events',str(args.detailed_events)]+(['--kv-cache-memory-bytes',str(args.kv_cache_memory_bytes)] if args.kv_cache_memory_bytes else []),stdout=f,stderr=f,stdin=subprocess.DEVNULL,start_new_session=True)
  print(json.dumps({'state':'launching','pid':proc.pid,'directory':str(p)}));return
 if args.action=='launch-worker':
  code=subprocess.call([str(B/'scripts/start_vllm.sh'),'--start','--context-window',str(args.context_window),'--detailed-events',str(args.detailed_events)]+(['--kv-cache-memory-bytes',str(args.kv_cache_memory_bytes)] if args.kv_cache_memory_bytes else []));write(p/'launch.json',{'returncode':code,'finished_ns':now()});return
 if args.action=='launch-status':
  print((p/'launch.json').read_text() if (p/'launch.json').exists() else json.dumps({'state':'launching'}));return
 if args.action=='start':
  if args.metrics_interval not in (0,.5,1,2,5):
   raise ValueError('Metrics interval must be 0,0.5,1,2 or 5 seconds')
  ssh_parts=os.environ.get('SSH_CONNECTION','').split()
  p.mkdir(parents=True,exist_ok=False);(p/'lease').touch();write(p/'state.json',{
   'run_id':args.run_id,'task_id':args.task_id,'state':'STARTING',
   'client_ip':ssh_parts[0] if ssh_parts else None,
   'ssh_capture_control_connection':dict(zip(['client_ip','client_port','server_ip','server_port'],ssh_parts)),
   'ssh_tunnel_local_port':args.tunnel_local_port,'ssh_tunnel_client_pid':args.tunnel_pid,
   'ssh_identity_limitation':'Remote tunnel TCP source port is not the control SSH connection',
   'metrics_enabled':bool(args.metrics_interval),'tcp_interval_seconds':args.tcp_interval,'detailed_events':bool(args.detailed_events)})
  with (p/'worker.log').open('wb') as f:subprocess.Popen([sys.executable,__file__,'worker','--path',str(p),'--max-seconds',str(min(max(args.max_seconds,30),86400)),'--metrics-interval',str(args.metrics_interval),'--tcp-interval',str(args.tcp_interval),'--detailed-events',str(args.detailed_events)],stdout=f,stderr=f,stdin=subprocess.DEVNULL,start_new_session=True,close_fds=True)
  for _ in range(150):
   s=json.loads((p/'state.json').read_text())
   if s['state']=='READY':print(json.dumps(s));return
   if s['state']=='FAILED':raise RuntimeError(str(s))
   time.sleep(.1)
  raise RuntimeError('Capture initialization timeout')
 if not p.exists():raise RuntimeError('No capture session')
 if args.action=='status':
  (p/'lease').touch();s=json.loads((p/'state.json').read_text());s['model']=model(False);print(json.dumps(s));return
 if args.action in ['stop','collect']:
  (p/'stop').touch()
  for _ in range(200):
   s=json.loads((p/'state.json').read_text())
   if s['state'] in ['STOPPED','FAILED']:break
   time.sleep(.1)
  else:terminate_capture(p);raise RuntimeError('Capture stop unconfirmed')
  if args.action=='stop':print(json.dumps(s));return
  subprocess.run([sys.executable,str(B/'scripts/parse_server_capture.py'),str(p)],check=True,timeout=120,capture_output=True)
  s=json.loads((p/'state.json').read_text());s['files']={n:{'bytes':(p/n).stat().st_size,'sha256':file_sha256(p/n)} for n in ['server_traffic.pcapng','server_timeline.jsonl','metrics_samples.jsonl','tcp_samples.jsonl','v3_events.jsonl','v3_telemetry_collection.json'] if (p/n).exists()};s['capture_log']=(p/'capture.log').read_text(errors='replace');s['analysis']=json.loads((p/'analysis.json').read_text());write(p/'collected.json',s);print(json.dumps(s))
if __name__=='__main__':main()
