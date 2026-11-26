#!/usr/bin/env python3
import importlib.util,pathlib,json,time,datetime,subprocess,urllib.request,traceback
B=pathlib.Path('/home/huanglabspk/WaiTung')
src=(B/'scripts/validate_vllm_live.py').read_text().replace('urlopen(req,timeout=30)','urlopen(req,timeout=180)')
m=type(__import__('sys'))('controlled_base');exec(compile(src,'controlled_base','exec'),m.__dict__)
stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');root=B/'deployment/controlled-validation'/stamp;root.mkdir(parents=True)
model=m.ins('waitung-vllm-qwen36');run={'run_id':stamp,'started_utc':m.utc(),'model_container_id':model['Id'],'tests':{k:{'status':'not_run'} for k in ['cold_chat','warm_chat','responses','codex_agent','humaneval']},'policy':{'socket_timeout_seconds':180,'total_deadline_seconds':190,'auto_retries':0,'single_controlled_start':True}}
m.atomic(root/'run.json',run);(B/'deployment/latest-api-validation-run.txt').write_text(str(root));s=m.Session();s.save()
try:
 s.stage='startup_wait'
 deadline=time.monotonic()+1200
 while True:
  s.snapshot()
  try:
   with urllib.request.urlopen('http://127.0.0.1:8000/health',timeout=2) as r:
    if r.status==200:break
  except Exception:pass
  if time.monotonic()>deadline:raise RuntimeError('Startup health timeout 1200s')
  time.sleep(2)
 for stage in ['cold_chat','warm_chat','responses']:
  s.stage=stage;t=s.run['tests'][stage];t['status']='running';t['started_utc']=m.utc();t['resources_before']=s.gate();s.save()
  if stage=='responses':path='/v1/responses';payload={'model':'Qwen3.6-35B-A3B','input':'Reply OK.','max_output_tokens':16,'temperature':0,'store':False,'chat_template_kwargs':{'enable_thinking':False}}
  else:path='/v1/chat/completions';payload={'model':'Qwen3.6-35B-A3B','messages':[{'role':'user','content':'Reply OK.'}],'max_tokens':16,'temperature':0,'chat_template_kwargs':{'enable_thinking':False}}
  r=s.http(stage,path,payload,deadline=190)
  if stage=='responses':text=''.join(c.get('text','') for o in r['body'].get('output',[]) for c in o.get('content',[]));assert r['body'].get('status')=='completed',r
  else:text=r['body']['choices'][0]['message'].get('content') or ''
  assert text.strip(),r
  t.update({'status':'passed','text':text,'elapsed_seconds':r['elapsed_seconds'],'first_response_headers_seconds':r['headers_seconds'],'first_body_byte_seconds':r.get('first_body_byte_seconds'),'usage':r['body'].get('usage'),'resources_after':s.snapshot(),'server_log':s.logserver(stage,t['started_utc'])});s.save()
 s.run['api_phase']='passed';s.save()
except BaseException as e:
 t=s.run['tests'].get(s.stage)
 if t is not None:t.update({'status':'failed','error':repr(e),'traceback':traceback.format_exc()})
 s.stop(repr(e));s.run['api_phase']='failed';s.save()
finally:
 s.logserver('full')
 try:s.run['final_resources']=s.snapshot(check=False)
 except Exception as e:s.run['final_resource_error']=repr(e)
 s.save()
 print(json.dumps(s.run,indent=2),flush=True)
