#!/usr/bin/env python3
"""Bounded sequential API validation with host safety checks. No model restart/config change."""
import argparse,datetime,json,multiprocessing,pathlib,shutil,subprocess,time,traceback,urllib.request,urllib.error
from vllm_validation_policy import evaluate
B=pathlib.Path('/home/huanglabspk/WaiTung');G=1024**3;VP=B/'deployment/validation.json'
STAGES=['models','chat_text','chat_sse','chat_tools','responses_text','responses_sse','codex_agent']
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def atomic(p,v):
 q=p.with_suffix(p.suffix+'.tmp');q.write_text(json.dumps(v,indent=2,ensure_ascii=False));q.replace(p)
def ins(n):return json.loads(subprocess.check_output(['docker','inspect',n],timeout=5))[0]
def worker(endpoint,payload,stream,dest):
 start=time.monotonic();result={'endpoint':endpoint,'started_utc':utc()}
 try:
  req=urllib.request.Request('http://127.0.0.1:8000'+endpoint,data=None if payload is None else json.dumps(payload).encode(),headers={'Content-Type':'application/json','Accept':'text/event-stream' if stream else 'application/json'})
  with urllib.request.urlopen(req,timeout=30) as r:
   result.update({'http_status':r.status,'content_type':r.headers.get('Content-Type'),'headers_seconds':time.monotonic()-start})
   if stream:
    result['events']=[];result['done_sentinel']=False
    first=r.read(1);result['first_body_byte_seconds']=time.monotonic()-start if first else None
    pending=first;data=[];event_type=None
    while pending:
     line=pending+r.readline();pending=b'X';line=line.decode('utf-8').rstrip('\r\n')
     if line.startswith('data:'):data.append(line[5:].lstrip())
     elif line.startswith('event:'):event_type=line[6:].strip()
     elif not line and data:
      raw='\n'.join(data)
      if raw=='[DONE]':result['done_sentinel']=True;break
      result['events'].append({'arrival_seconds':time.monotonic()-start,'event':event_type,'data':json.loads(raw)});data=[];event_type=None
     nxt=r.read(1)
     if not nxt:break
     pending=nxt
    if data:
     raw='\n'.join(data)
     if raw=='[DONE]':result['done_sentinel']=True
     else:result['events'].append({'arrival_seconds':time.monotonic()-start,'event':event_type,'data':json.loads(raw)})
   else:
    first=r.read(1);result['first_body_byte_seconds']=time.monotonic()-start if first else None
    result['body']=json.loads(first+r.read(2*1024*1024))
 except urllib.error.HTTPError as e:result.update({'http_status':e.code,'error_body':e.read(65536).decode(errors='replace')})
 except BaseException as e:result.update({'error':repr(e),'traceback':traceback.format_exc()})
 result['elapsed_seconds']=time.monotonic()-start;atomic(pathlib.Path(dest),result)
class SafetyError(RuntimeError):pass
class ServerError(RuntimeError):pass
class Session:
 def __init__(self):
  self.root=pathlib.Path((B/'deployment/latest-api-validation-run.txt').read_text().strip());self.p=self.root/'run.json';self.run=json.loads(self.p.read_text());self.stage='setup';self.hist=[]
 def save(self):
  atomic(self.p,self.run);v=json.loads(VP.read_text()) if VP.exists() else {};v['live_api_validation']=self.run;atomic(VP,v)
 def logserver(self,name,since=None):
  cmd=['docker','logs','--timestamps']
  if since:cmd+=['--since',since]
  cmd+=['waitung-vllm-qwen36']
  p=self.root/(name+'.server.log')
  with p.open('wb') as f:
   try:subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,timeout=12)
   except subprocess.TimeoutExpired:f.write(b'\nLog collection timed out\n')
  return str(p)
 def snapshot(self,check=True):
  m=ins('waitung-vllm-qwen36');w=ins('waitung-vllm-watchdog');mem={k:int(v.split()[0])*1024 for k,v in (l.split(':',1) for l in pathlib.Path('/proc/meminfo').read_text().splitlines())}
  psi={}
  for l in pathlib.Path('/proc/pressure/memory').read_text().splitlines():
   parts=l.split();psi[parts[0]]={k:float(v) for k,v in (x.split('=') for x in parts[1:])}
  guard=pathlib.Path(next(x['Source'] for x in m['Mounts'] if x['Destination']=='/guard'));hb=json.loads((guard/'status.json').read_text());ev={};cg={}
  if m['State']['Running']:
   rel=next(l.split(':',2)[2] for l in pathlib.Path(f"/proc/{m['State']['Pid']}/cgroup").read_text().splitlines() if l.startswith('0::'));cp=pathlib.Path('/sys/fs/cgroup')/rel.lstrip('/')
   ev={k:int(v) for k,v in (x.split() for x in (cp/'memory.events').read_text().splitlines())}
   for k in ['memory.current','memory.max','memory.swap.current','memory.swap.max']:cg[k]=(cp/k).read_text().strip()
  s={'utc':utc(),'monotonic':time.monotonic(),'stage':self.stage,'mem_available_bytes':mem['MemAvailable'],'swap_used_bytes':mem['SwapTotal']-mem['SwapFree'],'memory_psi':psi,'model_state':m['State'],'model_id':m['Id'],'watchdog_state':w['State'],'watchdog_id':w['Id'],'heartbeat_age_seconds':time.monotonic()-hb['monotonic'],'heartbeat_state':hb['state'],'guard_directory':str(guard),'memory_events':ev,'cgroup':cg}
  with (self.root/'resources.jsonl').open('a') as f:f.write(json.dumps(s)+'\n')
  reason=None
  if m['Id']!=self.run['model_container_id'] or m['Config']['Labels'].get('owner')!='waitung-qwen36':reason='Model identity/ownership changed'
  elif not m['State']['Running'] or m['State']['OOMKilled']:reason='Model stopped or OOMKilled'
  elif not w['State']['Running'] or w['State']['OOMKilled'] or w['Config']['Labels'].get('target-id')!=m['Id']:reason='Watchdog stopped/OOM/target mismatch'
  elif hb['state']!='armed' or s['heartbeat_age_seconds']>8 or hb['target_id']!=m['Id']:reason='Watchdog heartbeat unsafe/stale'
  elif mem['MemAvailable']<=25*G:reason='MemAvailable at or below 25 GiB'
  elif any(ev.get(k,0)>0 for k in ['oom','oom_kill','oom_group_kill']):reason='Container cgroup OOM event'
  self.hist=[x for x in self.hist if s['monotonic']-x['monotonic']<=30]
  reason,warnings=evaluate(s,self.hist,reason)
  for warning in warnings:
   with (self.root/'warnings.jsonl').open('a') as f:f.write(json.dumps({'utc':s['utc'],'stage':self.stage,**warning})+'\n')
  self.hist.append(s)
  if reason and check:raise SafetyError(reason)
  return s
 def stop(self,reason):
  self.run['aborted']=True;self.run['abort_reason']=reason;self.save()
  with (self.root/'safety-stop.log').open('w') as f:
   try:r=subprocess.run([str(B/'scripts/stop_vllm.sh')],stdout=f,stderr=subprocess.STDOUT,timeout=25);self.run['stop_script_exit_code']=r.returncode
   except Exception as e:self.run['stop_script_error']=repr(e)
  self.logserver('abort-full');self.save()
 def gate(self):
  s=self.snapshot();time.sleep(2);self.snapshot()
  with urllib.request.urlopen('http://127.0.0.1:8000/health',timeout=8) as r:
   if r.status!=200:raise ServerError('Health not 200')
  return s
 def http(self,name,path,data=None,stream=False,deadline=60):
  f=self.root/(name+'.response.json');atomic(self.root/(name+'.request.json'),{'path':path,'payload':data,'stream':stream,'deadline_seconds':deadline})
  proc=multiprocessing.Process(target=worker,args=(path,data,stream,str(f)));proc.start();start=time.monotonic()
  try:
   while proc.is_alive():
    self.snapshot()
    if time.monotonic()-start>deadline:raise ServerError(f'HTTP total timeout {deadline}s')
    proc.join(timeout=1)
  except BaseException:
   proc.terminate();proc.join(timeout=3);raise
  if not f.exists():raise ServerError('HTTP worker exited without result')
  r=json.loads(f.read_text());self.run['tests'][self.stage].setdefault('requests',[]).append({'name':name,'response_file':str(f),'elapsed_seconds':r.get('elapsed_seconds'),'http_status':r.get('http_status')});self.save()
  if r.get('error'):raise ServerError(r['error'])
  if r.get('http_status',0)>=500:raise ServerError(str(r))
  if r.get('http_status')!=200:raise AssertionError(f"HTTP {r.get('http_status')}: {r.get('error_body')}")
  if stream:
   if 'text/event-stream' not in (r.get('content_type') or ''):raise AssertionError('Wrong SSE Content-Type')
   if any(x['data'].get('type') in ['error','response.failed'] or 'error' in x['data'] for x in r['events']):raise ServerError('SSE error event')
  self.snapshot();return r
 def execute(self,stage):
  self.stage=stage;t=self.run['tests'][stage]
  if t['status']!='not_run':raise SystemExit('Refusing to repeat an executed stage')
  if self.run.get('aborted'):raise SystemExit('Session aborted; refusing more requests')
  start=time.monotonic();t.update({'status':'running','started_utc':utc()});self.save()
  try:
   t['resources_before']=self.gate();model=self.run.get('model_id','Qwen3.6-35B-A3B');kw={'model':model,'temperature':0,'chat_template_kwargs':{'enable_thinking':False}}
   if stage=='models':
    r=self.http('models','/v1/models',deadline=15)['body'];ids=[x['id'] for x in r['data']];assert 'Qwen3.6-35B-A3B' in ids,ids;self.run['model_id']='Qwen3.6-35B-A3B';t['model_ids']=ids
   elif stage=='chat_text':
    r=self.http(stage,'/v1/chat/completions',{**kw,'messages':[{'role':'user','content':'Reply OK.'}],'max_tokens':16})['body'];text=r['choices'][0]['message'].get('content');assert text and text.strip(),r;t['text']=text;t['usage']=r.get('usage')
   elif stage=='chat_sse':
    r=self.http(stage,'/v1/chat/completions',{**kw,'messages':[{'role':'user','content':'Say one two three.'}],'max_tokens':32,'stream':True},True);texts=[e['data']['choices'][0].get('delta',{}).get('content','') for e in r['events'] if e['data'].get('choices')];assert any(texts) and r['done_sentinel'],r;t.update({'event_count':len(r['events']),'content_delta_count':sum(bool(x) for x in texts),'text':''.join(texts),'first_body_byte_seconds':r['first_body_byte_seconds']})
   elif stage=='chat_tools':
    tool={'type':'function','function':{'name':'get_weather','description':'Return weather for a city.','parameters':{'type':'object','properties':{'city':{'type':'string'}},'required':['city'],'additionalProperties':False}}};msgs=[{'role':'user','content':'Call get_weather for Beijing.'}]
    r=self.http(stage,'/v1/chat/completions',{**kw,'messages':msgs,'tools':[tool],'tool_choice':'auto','max_tokens':96})['body'];msg=r['choices'][0]['message'];calls=msg.get('tool_calls') or [];assert len(calls)==1,r;call=calls[0];assert call['function']['name']=='get_weather';args=json.loads(call['function']['arguments']);assert isinstance(args.get('city'),str) and args['city'].lower() in ['beijing','??'],args;t['tool_call']=call
    # Synthetic, side-effect-free tool output; this is the only tool-return request.
    msgs+=[msg,{'role':'tool','tool_call_id':call['id'],'content':'{"city":"Beijing","weather":"sunny"}'}]
    second=self.http('chat_tool_return','/v1/chat/completions',{**kw,'messages':msgs,'tools':[tool],'max_tokens':32})['body'];text=second['choices'][0]['message'].get('content') or '';assert text.strip(),second;t['tool_result_response']=text
   elif stage in ['responses_text','responses_sse']:
    stream=stage=='responses_sse';r=self.http(stage,'/v1/responses',{**kw,'input':'Say OK.','max_output_tokens':32,'store':False,'stream':stream},stream)
    if not stream:
     body=r['body'];texts=[c.get('text','') for o in body.get('output',[]) for c in o.get('content',[])];assert body.get('status')=='completed' and any(texts),body;t['text']=''.join(texts);t['response_status']=body['status']
    else:
     types=[e['data'].get('type') for e in r['events']];assert 'response.completed' in types and 'response.output_text.delta' in types,types;t.update({'event_count':len(types),'event_types':types,'first_body_byte_seconds':r['first_body_byte_seconds']})
   else:raise AssertionError('Agent is handled separately after API checks')
   t['resources_after']=self.snapshot();t['status']='passed'
  except BaseException as e:
   t.update({'status':'failed','error':str(e),'error_type':type(e).__name__,'traceback':traceback.format_exc()})
   if isinstance(e,(SafetyError,ServerError,urllib.error.URLError,subprocess.TimeoutExpired)):self.stop(str(e))
  finally:
   t['elapsed_seconds']=time.monotonic()-start;t['server_log']=self.logserver(stage,t['started_utc']);self.run['last_observed_utc']=utc()
   try:self.run['last_resources']=self.snapshot(check=False)
   except Exception as e:self.run['resource_read_error']=repr(e)
   self.save();print(json.dumps({'stage':stage,'status':t['status'],'error':t.get('error'),'elapsed_seconds':t['elapsed_seconds'],'result_directory':str(self.root),'aborted':self.run.get('aborted',False)},indent=2),flush=True)
def main():
 p=argparse.ArgumentParser();p.add_argument('stage',choices=['init']+STAGES[:-1]);a=p.parse_args()
 if a.stage=='init':
  stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');root=B/'deployment/live-validation'/stamp;root.mkdir(parents=True)
  if VP.exists():shutil.copy2(VP,root/'validation.before.json')
  model=ins('waitung-vllm-qwen36');run={'run_id':stamp,'started_utc':utc(),'scope':'bounded deployment/API compatibility validation only','model_container_id':model['Id'],'tests':{x:{'status':'not_run'} for x in STAGES},'policy':{'min_mem_available_gib':25,'http_deadline_seconds':60,'parallel_inference':False,'auto_retries':0,'model_restart':False}}
  atomic(root/'run.json',run);(B/'deployment/latest-api-validation-run.txt').write_text(str(root));s=Session();s.save();s.logserver('initial-full')
  try:s.stage='initial_gate';run=s.run;run['initial_resources']=s.gate();run['initial_gate']='passed';s.save();print(json.dumps({'initial_gate':'passed','root':str(root),'mem_available_gib':run['initial_resources']['mem_available_bytes']/G},indent=2))
  except Exception as e:s.stop('Initial safety/health gate failed: '+str(e));raise
 else:Session().execute(a.stage)
if __name__=='__main__':main()
