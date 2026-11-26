"""Canonical server timeline and task summary, independent of dataset/harness.
Legacy reconciliation uses the old parser's one-record-per-source-row contract,
exact native identity, and field equality; ambiguous evidence fails closed.
"""
from collections import Counter, defaultdict
from pathlib import Path
import json, hashlib, re, math, os

CORE_FILES=('raw_traffic.pcapng','server_traffic.pcapng','codex_timeline.jsonl','server_timeline.jsonl','codex_output.json','task_prompt.txt','result.json')
EXTRA_FILES=('metrics_samples.jsonl','tcp_samples.jsonl','v3_events.jsonl','v3_telemetry_collection.json')
SCHEMA='agent-traffic-unified-1'
_CHILD=re.compile(r'^(resp_[A-Za-z0-9_-]+)_([0-9]+)-([0-9a-fA-F]+)$')
def parent_response_id(request_id):
    if not isinstance(request_id,str):return None
    match=_CHILD.fullmatch(request_id)
    return match.group(1) if match else request_id

def canonical(value):return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':'))
def equal(a,b):return canonical(a)==canonical(b)
def iter_jsonl(path):
    with Path(path).open(encoding='utf-8') as f:
        for i,line in enumerate(f,1):
            try:x=json.loads(line)
            except ValueError as e:raise ValueError(f'{path}:{i}: {e}') from e
            if not isinstance(x,dict):raise ValueError(f'{path}:{i}: non-object')
            yield i,x

def normalize_event(e):
    out=dict(e); source=out.get('source');kind=out.get('type') or out.get('event_type')
    out.setdefault('original_schema_version',out.get('schema_version'));out['schema_version']=SCHEMA
    # Preserve original SSE protocol event_type, not the generic packet kind.
    out.setdefault('event_type',kind);out.setdefault('type',kind)
    out.setdefault('timestamp_ns',out.get('observed_ns'));out.setdefault('observed_ns',out.get('timestamp_ns'))
    out.setdefault('clock','Spark CLOCK_REALTIME' if out.get('timestamp_ns') is not None else None)
    source_map={'server_pcapng':'packet_capture','spark_capture_state':'measurement_control','vllm_prometheus_scrape':'prometheus'}
    new=source_map.get(source,source)
    if new=='api_server' and kind in ('asgi_send_start','asgi_send_end'):new='asgi'
    if source!=new:out.setdefault('original_source',source);out['source']=new
    if new in ('prometheus','tcp_info','measurement_control') or out.get('measurement_control'):
        out['correlation_status']='not_applicable'
    else:out.setdefault('correlation_status',out.get('link_status') or ('matched' if out.get('model_request_id') else 'unmatched'))
    if new in ('scheduler','engine_output') and out.get('request_id'):
        out.setdefault('original_request_id',out['request_id']);out.setdefault('parent_response_id',parent_response_id(out['request_id']))
    return out

def sample_statistics(records,start_ns=None,end_ns=None,configured_interval=None,uncertainty_ns=None):
    if configured_interval is None and records:configured_interval=records[0].get('interval_seconds')
    starts=sorted(e.get('scrape_start_ns',e.get('sample_start_ns',e.get('observed_ns'))) for e in records if isinstance(e.get('scrape_start_ns',e.get('sample_start_ns',e.get('observed_ns'))),int))
    ends=sorted(e['observed_ns'] for e in records if isinstance(e.get('observed_ns'),int))
    mono=[e.get('sample_start_monotonic_ns') for e in records]
    clock='CLOCK_MONOTONIC' if records and all(isinstance(t,int) for t in mono) else 'CLOCK_REALTIME (legacy; clock steps possible)'
    t=sorted(mono) if clock=='CLOCK_MONOTONIC' else starts
    gaps=[(b-a)/1e9 for a,b in zip(t,t[1:])]
    def q(values,f):
        if not values:return None
        a=sorted(values);i=(len(a)-1)*f;lo=int(i);return a[lo]+(a[min(lo+1,len(a)-1)]-a[lo])*(i-lo)
    durations=[x/1e9 for e in records if isinstance(x:=e.get('scrape_duration_ns',e.get('sample_duration_ns')),int)]
    first,last=(starts[0] if starts else None),(ends[-1] if ends else None)
    bracket=None if None in (first,last,start_ns,end_ns,uncertainty_ns) else first<=start_ns-uncertainty_ns and last>=end_ns+uncertainty_ns
    return {'count':len(records),'configured_interval_seconds':configured_interval,'interval_clock':clock,
      'first_sample_ns':first,'last_sample_ns':last,'first_completion_ns':ends[0] if ends else None,'last_completion_ns':last,
      'coverage_seconds':(last-first)/1e9 if first is not None and last is not None else None,
      'completion_span_seconds':(ends[-1]-ends[0])/1e9 if ends else None,
      'agent_start_remote_estimated_ns':start_ns,'agent_end_remote_estimated_ns':end_ns,'clock_uncertainty_ns':uncertainty_ns,
      'agent_interval_bracketed_with_uncertainty':bracket,'agent_active_seconds':(end_ns-start_ns)/1e9 if start_ns is not None and end_ns is not None else None,
      'first_sample_after_agent_start_seconds':(first-start_ns)/1e9 if first is not None and start_ns is not None else None,
      'last_sample_before_agent_end_seconds':(end_ns-last)/1e9 if last is not None and end_ns is not None else None,
      'interval_p50_seconds':q(gaps,.5),'interval_p95_seconds':q(gaps,.95),'interval_max_seconds':max(gaps,default=None),
      'duration_p50_seconds':q(durations,.5),'duration_p95_seconds':q(durations,.95),'duration_max_seconds':max(durations,default=None),
      'failures':sum(bool(e.get('error')) for e in records),'missed_cycles':None}

DIRECT={'request_receive_start','request_body_complete','request_validation_done','request_parse_ready_for_engine','responses_handler_start','response_headers_ready','engine_enqueue','scheduled_chunk','engine_core_output_available','output_processor_received','api_queue_delivered','sse_encode_start','sse_encode_done','stream_yield','asgi_send_start','asgi_send_end'}
NEVER={'gpu_token_kernel_complete','socket_write_complete','kernel_tcp_tx_timestamp','kernel_tcp_ack_timestamp','http_json_parsed_exact','prefill_gpu_kernel_start_end'}
def availability(events,enabled=None,failed=False):
    observed={e.get('type') or e.get('event_type') for e in events};missing=DIRECT-observed
    return {'supported_direct':sorted(DIRECT),'observed':sorted(DIRECT&observed),
      'supported_not_triggered':sorted(missing) if enabled is True and not failed else [],
      'supported_not_observed_reason_unknown':sorted(missing) if enabled is None and not failed else [],
      'disabled':sorted(missing) if enabled is False else [],'collection_failed':sorted(missing) if failed else [],
      'not_implemented':sorted(NEVER),'offline_derived':['request_classification','pcap_http_sse_timestamps'],
      'aggregate_metrics_observed':'vllm_metrics_sample' in observed,'connection_samples_observed':'tcp_host_ssh_snapshot' in observed}

def classification_summary(events):
    rows={};resp=defaultdict(list);sse=defaultdict(list)
    for e in events:
        rid=e.get('model_request_id');t=e.get('type')
        if not rid:continue
        if t=='http_request_end' and e.get('path','').split('?')[0] in ('/v1/responses','/v1/responses/compact') and e.get('method')=='POST':rows[rid]=e
        elif t=='http_response_end' and e.get('status',0)>=200:resp[rid].append(e)
        elif t=='sse_event_observed':sse[rid].append(e)
    cats=Counter(); details={}
    for rid,e in rows.items():
        category=e.get('request_category','unknown');cats[category]+=1
        r=resp[rid][0] if len(resp[rid])==1 else {}; ss=sse[rid]
        usage=[x for x in ss if x.get('event_type') in ('response.completed','response.failed','response.incomplete') and isinstance(x.get('input_tokens'),int) and isinstance(x.get('output_tokens'),int)]
        u=usage[0] if len(usage)==1 else {};start=e.get('request_body_complete_observed_time');end=r.get('response_body_complete_observed_time')
        complete=r.get('response_complete') is True and (not str(r.get('content_type','')).startswith('text/event-stream') or any(x.get('event_type') in ('response.completed','response.failed','response.incomplete') for x in ss))
        details[rid]={'category':category,'evidence':e.get('classification_evidence'),'input_tokens':u.get('input_tokens'),'output_tokens':u.get('output_tokens'),'upload_bytes':e.get('http_wire_bytes'),'download_bytes':r.get('http_wire_bytes'),'observed_response_duration_seconds':(end-start)/1e9 if complete and isinstance(start,int) and isinstance(end,int) and end>=start else None,'complete':complete}
    c=[v for v in details.values() if v['category']=='compaction']; totals={}
    for k in ('input_tokens','output_tokens','upload_bytes','download_bytes','observed_response_duration_seconds'):
        known=[v[k] for v in c if v[k] is not None];totals[k]=sum(known) if known or not c else None;totals[k+'_known_requests']=len(known)
    totals.update(complete=sum(v['complete'] for v in c),incomplete=sum(not v['complete'] for v in c))
    return {'total':len(rows),'normal':cats['normal'],'compaction':cats['compaction'],'unknown':cats['unknown'],'compaction_usage':totals,'requests':details,'byte_semantics':'HTTP wire bytes incl HTTP framing; not TCP frames','duration_semantics':'request body complete to response body complete, complete responses only'}

def summarize(events,measurement):
    cov=measurement.get('coverage') or {};off=cov.get('offset_remote_minus_local_ns');bound=cov.get('uncertainty_bound_ns')
    start=measurement.get('agent_start_ns');end=measurement.get('agent_end_ns')
    start=start+off if isinstance(start,int) and isinstance(off,int) else None;end=end+off if isinstance(end,int) and isinstance(off,int) else None
    a=(measurement.get('server_collection') or {}).get('analysis',{})
    for key,kind,interval_key in [('metrics','vllm_metrics_sample','metrics_sampling_interval_seconds'),('tcp','tcp_host_ssh_snapshot','tcp_sampling_interval_seconds')]:
        records=[e.get('raw_sample',e.get('tcp_snapshot',{})) for e in events if e.get('type')==kind]
        stats=sample_statistics(records,start,end,measurement.get(interval_key),bound)
        prior=measurement.get(key) or {};stats['missed_cycles']=prior.get('missed_cycles',(a.get('sampling',{}).get('prometheus' if key=='metrics' else 'tcp') or {}).get('missed_cycles'))
        if key=='metrics':stats.update(raw_text_available=sum(isinstance(r.get('raw_text'),str) for r in records),counter_resets=sum(len(r.get('counter_resets',[])) for r in records),service_restarts=sum(bool(r.get('service_process_changed')) for r in records))
        measurement[key]={**prior,**stats}
    native=[e for e in events if e.get('source_record_file')=='v3_events.jsonl' or e.get('observed_from_file')]
    noncontrol=[e for e in native if e.get('source')!='measurement_control'];counts=Counter(e.get('correlation_status') for e in noncontrol)
    measurement['correlation']={'method':'explicit IDs, deterministic child-parent; no time fallback','non_control_events':len(noncontrol),'matched':counts['matched'],'unmatched':counts['unmatched'],'ambiguous':counts['ambiguous'],'other':len(noncontrol)-sum(counts[k] for k in ('matched','unmatched','ambiguous')),'match_rate':counts['matched']/len(noncontrol) if noncontrol else None}
    measurement['availability']=availability(events,measurement.get('detailed_events'),bool(a.get('v3_parse_errors') or (measurement.get('telemetry') or {}).get('error')))
    measurement['model_request_classification']=classification_summary(events)
    measurement['telemetry']={**(measurement.get('telemetry') or {}),'native_event_count':len(native),'event_counts':dict(Counter(e.get('source') for e in events))}
    return measurement

def _native_key(e):
    keys=('observed_from_file','pid','monotonic_ns','timestamp_ns','event_type')
    if all(e.get(k) is not None for k in keys):return tuple(e[k] for k in keys)
    # Synthetic and older writers without file tag: require exact native clocks + process + source + type.
    keys=('pid','monotonic_ns','timestamp_ns','source','event_type')
    return tuple(e[k] for k in keys) if all(e.get(k) is not None for k in keys) else None

def merge_task(task_dir):
    p=Path(task_dir)
    if any(not (p/n).is_file() for n in CORE_FILES):raise FileNotFoundError('All seven base artifacts required')
    result=json.loads((p/'result.json').read_text());m=dict(result.get('measurement') or {})
    existing=[x for _,x in iter_jsonl(p/'server_timeline.jsonl')];events=[dict(x) for x in existing];n0=len(events)
    scope=next((e for e in events if e.get('type')=='scope'),{})
    run=scope.get('run_id') or next((e.get('run_id') for e in events if e.get('run_id')),m.get('run_id'));task=scope.get('task_id') or next((e.get('task_id') for e in events if e.get('task_id')),m.get('task_id'))
    # Historical source records are mapped by exact identities/projections; no nearest-time match.
    counts={};native_index=defaultdict(list)
    for e in events:
        if (key:=_native_key(e)) is not None:native_index[key].append(e)
    for filename,kind in [('metrics_samples.jsonl','vllm_metrics_sample'),('tcp_samples.jsonl','tcp_host_ssh_snapshot'),('v3_events.jsonl',None)]:
        if not (p/filename).exists():continue
        raw=list(iter_jsonl(p/filename));counts[filename]=len(raw)
        legacy=[e for e in events if e.get('type')==kind] if kind else []
        used=set()
        for i,r in raw:
            if kind:
                # Old parser produced a one-to-one projection. Require exact fields and a unique match.
                if kind=='vllm_metrics_sample':
                    candidates=[e for e in legacy if e.get('scrape_start_ns')==r.get('scrape_start_ns') and e.get('scrape_end_ns')==r.get('observed_ns') and ('metrics' not in e or equal(e['metrics'],r.get('metrics',[])))]
                else:candidates=[e for e in legacy if equal(e.get('tcp_snapshot',e.get('raw_sample')),r)]
            else:
                key=_native_key(r);candidates=native_index.get(key,[]) if key is not None else []
                if not candidates and key is None:raise ValueError('missing native identity')
                for e in candidates:
                    if any(not equal(e.get(k),v) for k,v in r.items()):raise ValueError('native identity collision or changed raw fields')
            if len(candidates)>1:raise ValueError(f'ambiguous raw identity {filename}:{i}')
            if candidates:
                e=candidates[0]
                if id(e) in used:raise ValueError(f'repeated raw identity {filename}:{i}')
                used.add(id(e))
                if 'raw_sample' in e and not equal(e['raw_sample'],r):raise ValueError('raw sample conflict')
            else:
                if not kind: # Native record missing from legacy timeline: retain exact record, explicitly unlinked.
                    e=dict(r);e.update(type=r.get('event_type'),observed_ns=r.get('timestamp_ns'),correlation_status='unmatched')
                else:
                    e={'source':'prometheus' if kind=='vllm_metrics_sample' else 'tcp_info','type':kind,'event_type':kind,'observed_ns':r.get('observed_ns'),'timestamp_semantics':'aggregate scrape completion' if kind=='vllm_metrics_sample' else 'connection snapshot completion','measurement_control':True}
                    if kind=='tcp_host_ssh_snapshot':e['tcp_snapshot']=r
                    else:e.update(metrics=r.get('metrics',[]),scrape_start_ns=r.get('scrape_start_ns'),scrape_end_ns=r.get('observed_ns'))
                e.update(run_id=run,task_id=task);events.append(e)
            if kind:e['raw_sample']=r
            e.update(source_record_file=filename,source_record_line=i,source_record_sha256=hashlib.sha256(canonical(r).encode()).hexdigest())
        # A partial file is not evidence of completeness; preserve unmatched existing events.
    # Recover historical raw Prometheus text and classify requests from exact frame/byte evidence.
    try:
        from .pcap_evidence import packets,body_for
        from .parse_server_capture import classify_compaction
    except ImportError:
        from pcap_evidence import packets,body_for
        from parse_server_capture import classify_compaction
    recovery=Counter();metric_bodies={}
    try:flows=packets(p/'server_traffic.pcapng',scope.get('canonical_interface','docker0'))
    except Exception as exc:flows=None;recovery['pcap_reader_unavailable']+=1;m['historical_pcap_recovery_error']=str(exc)
    if flows is not None:
        req={e.get('model_request_id'):e for e in events if e.get('type')=='http_request_end'}
        for e in req.values():
            if e.get('path','').split('?')[0] not in ('/v1/responses','/v1/responses/compact'):continue
            try:
                body=body_for(e,flows);category,evidence=classify_compaction(body);recovery['model_bodies_verified']+=1
                e.update(request_category=category,classification_evidence=evidence,request_body_sha256=hashlib.sha256(body).hexdigest())
            except Exception as exc:e.setdefault('request_category','unknown');e.setdefault('classification_evidence','PCAP recovery unavailable: '+str(exc));recovery['model_body_failures']+=1
        for e in events:
            if e.get('type')=='http_response_end' and req.get(e.get('model_request_id'),{}).get('path')=='/metrics':
                try:
                    body=body_for(e,flows);metric_bodies[hashlib.sha256(body).hexdigest()]=body.decode('utf-8')
                except Exception:recovery['metric_body_failures']+=1
        for e in events:
            if e.get('type')=='http_request_start' and (r:=req.get(e.get('model_request_id'))) and 'request_category' in r:
                for k in ('request_category','classification_evidence','request_body_sha256'):
                    if k in r:e[k]=r[k]
            if e.get('type')=='vllm_metrics_sample' and isinstance(r:=e.get('raw_sample'),dict) and 'raw_text' not in r:
                if (text:=metric_bodies.get(r.get('raw_sha256'))) is not None:
                    # Keep original sample unchanged; recovered content separate, exact hash matched.
                    e['recovered_raw_text']=text;e['raw_text_provenance']='exact SHA256 match to canonical HTTP /metrics response body';recovery['metrics_raw_text_recovered']+=1
                else:recovery['metrics_raw_text_unavailable']+=1
    meta=p/'v3_telemetry_collection.json'
    if meta.exists():m['telemetry']={**(m.get('telemetry') or {}),**json.loads(meta.read_text())}
    events=[normalize_event(e) for e in events]
    for e in events:
        if e.get('type')=='scope':e['unavailable']=sorted(NEVER)
    events.sort(key=lambda e:(e.get('timestamp_ns') is None,e.get('timestamp_ns') or 0))
    # Verify all original packet event counts and bytes survive unchanged.
    def network_signature(seq):
        return Counter(canonical({k:e.get(k) for k in ('type','model_request_id','first_frame','last_frame','frame_start','frame_end','http_wire_bytes','body_bytes','encoded_bytes','delta_bytes','sequence_number')}) for e in seq if e.get('type') in ('http_request_start','http_request_end','http_response_start','http_response_end','sse_event_observed'))
    if network_signature(existing)!=network_signature(events):raise ValueError('HTTP/SSE evidence changed')
    summarize(events,m)
    m['metrics']['raw_text_available']=sum(e.get('type')=='vllm_metrics_sample' and ('recovered_raw_text' in e or 'raw_text' in e.get('raw_sample',{})) for e in events)
    m['data_quality']={**(m.get('data_quality') or {}),'merge_status':'completed','merge_event_count':len(events),'historical_pcap_recovery':dict(recovery),'original_source_counts':counts or (m.get('data_quality') or {}).get('original_source_counts',{})}
    result['measurement']=m
    return events,result,{'existing':n0,'added':len(events)-n0,'merged':len(events),'source_counts':counts,'pcap_recovery':dict(recovery)}

def atomic_jsonl(path,events):
    path=Path(path);tmp=path.with_name(path.name+'.tmp')
    with tmp.open('w',encoding='utf-8') as f:
        for e in events:f.write(json.dumps(e,ensure_ascii=False,separators=(',',':'))+'\n')
        f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)
