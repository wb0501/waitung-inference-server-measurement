"""Controller-only safety policy. Independent watchdog is unchanged."""
G=1024**3
def evaluate(sample, history, reason=None):
 now=sample['monotonic'];warnings=[]
 recent=[s for s in history if 0<=now-s['monotonic']<=10]
 if recent:
  old=recent[0];dt=now-old['monotonic'];growth=sample['swap_used_bytes']-old['swap_used_bytes']
  if dt>0 and growth>=128*1024**2 and growth/dt>=16*1024**2:
   warnings.append({'type':'rapid_system_swap_growth','growth_mib':growth/1024**2,'interval_seconds':dt,'rate_mib_per_second':growth/dt/1024**2,'action':'record_only'})
 severe=lambda s:s['memory_psi'].get('full',{}).get('avg10',0)>=10 or s['memory_psi'].get('some',{}).get('avg10',0)>=30
 if sample['mem_available_bytes']<=25*G:reason=reason or 'MemAvailable at or below 25 GiB'
 if severe(sample):
  contiguous=[sample]
  for old in reversed(history):
   if not severe(old) or contiguous[-1]['monotonic']-old['monotonic']>4:break
   contiguous.append(old)
  if now-contiguous[-1]['monotonic']>=15:reason=reason or 'Sustained severe memory PSI for >=15s'
 if any(sample['memory_events'].get(k,0)>0 for k in ['oom','oom_kill','oom_group_kill']):reason=reason or 'Container cgroup OOM event'
 if not sample['model_state']['Running'] or sample['model_state']['OOMKilled']:reason=reason or 'Model stopped or OOMKilled'
 if not sample['watchdog_state']['Running'] or sample['heartbeat_state']!='armed' or sample['heartbeat_age_seconds']>8:reason=reason or 'Watchdog stopped or heartbeat unsafe/stale'
 return reason,warnings
