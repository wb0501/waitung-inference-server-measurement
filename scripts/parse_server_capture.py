#!/usr/bin/env python3
"""Offline HTTP/1.1 + Responses SSE reconstruction at ONE Docker bridge.

All packet timestamps are observations, never API/engine/socket-write times.
HTTP body sizes exclude chunk framing; an SSE event's encoded_bytes include
its SSE envelope, but exclude HTTP chunk framing. Duplicated interfaces are NOT summed.
"""
import bisect
try:
    from .telemetry_merge import parent_response_id, normalize_event, sample_statistics, classification_summary, availability, atomic_jsonl, NEVER
except ImportError:
    from telemetry_merge import parent_response_id, normalize_event, sample_statistics, classification_summary, availability, atomic_jsonl, NEVER
import collections
from decimal import Decimal
import hashlib
import json
import pathlib
import re
import subprocess
import sys

PARSER_VERSION = 'agent-traffic-http-sse-v2.1+unified-1'
UNAVAILABLE = ['http_json_parsed', 'engine_enqueue', 'prefill_start', 'prefill_complete',
               'token_generation', 'sse_encoded', 'streaming_response_yield',
               'socket_write', 'tcp_info_cwnd', 'tcp_info_pacing_rate',
               'tcp_info_delivery_rate', 'kernel_tx', 'kernel_ack']
FIELDS = ['frame.time_epoch', 'frame.interface_name', 'ip.src', 'tcp.srcport',
          'ip.dst', 'tcp.dstport', 'tcp.seq_raw', 'tcp.payload', 'frame.number',
          'tcp.stream', 'tcp.flags.syn', 'tcp.flags.fin', 'tcp.flags.reset']


def epoch_ns(raw):
    """Exact decimal conversion of tshark epoch strings (nanosecond precision)."""
    return int(Decimal(raw) * 1_000_000_000)


def _header(header_bytes):
    lines = header_bytes.decode('latin1').split('\r\n')
    hdr = {}
    for line in lines[1:]:
        if ':' in line:
            k, v = line.split(':', 1)
            hdr[k.lower()] = v.strip()
    return lines[0], hdr


def _message(data, cursor, stamp, issues, flow):
    """Parse a single HTTP message, tracking dechunked -> TCP direction offsets."""
    boundary = data.find(b'\r\n\r\n', cursor)
    if boundary < 0:
        if data[cursor:]:
            issues.append(f'Incomplete HTTP headers {flow} at {cursor}')
        return None
    line, headers = _header(data[cursor:boundary])
    if not re.match(r'HTTP/1\.[01] \d{3} |(?:GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH|CONNECT) \S+ HTTP/1\.[01]$', line):
        issues.append(f'Unexpected HTTP prefix {flow} at {cursor}: {line[:80]}')
        return None
    body_start = boundary + 4
    pos = body_start
    body_parts, maps, chunks = [], [], []
    length = 0
    complete = True
    if 'chunked' in headers.get('transfer-encoding', '').lower():
        while True:
            line_end = data.find(b'\r\n', pos)
            if line_end < 0:
                complete = False
                break
            try:
                chunk_len = int(data[pos:line_end].split(b';', 1)[0], 16)
                if chunk_len < 0:
                    raise ValueError('negative chunk')
            except ValueError:
                complete = False
                issues.append(f'Invalid HTTP chunk size {flow} at {pos}')
                break
            payload_start = line_end + 2
            if chunk_len == 0:
                # Includes zero chunk line and all optional trailers.
                tail = data.find(b'\r\n\r\n', pos)
                if tail == -1:
                    # No trailer fields: 0\r\n\r\n (blank line after zero chunk).
                    if data[payload_start:payload_start+2] == b'\r\n':
                        pos = payload_start + 2
                    else:
                        complete = False
                else:
                    pos = tail + 4
                break
            if payload_start + chunk_len + 2 > len(data):
                available = max(0, min(chunk_len, len(data) - payload_start))
                if available:
                    body_parts.append(data[payload_start:payload_start+available])
                    maps.append((length, length + available, payload_start))
                    length += available
                pos = len(data)
                complete = False
                break
            if data[payload_start+chunk_len:payload_start+chunk_len+2] != b'\r\n':
                complete = False
                issues.append(f'Invalid chunk terminator {flow} at {payload_start+chunk_len}')
                break
            body_parts.append(data[payload_start:payload_start+chunk_len])
            maps.append((length, length+chunk_len, payload_start))
            chunks.append((length, length+chunk_len, (payload_start-pos)+2))
            length += chunk_len
            pos = payload_start + chunk_len + 2
    elif 'content-length' in headers:
        try:
            n = int(headers['content-length'])
            if n < 0:
                raise ValueError('negative length')
        except ValueError:
            issues.append(f'Invalid Content-Length {flow} at {cursor}')
            return None
        end = min(body_start+n, len(data))
        body_parts = [data[body_start:end]]
        if end > body_start:
            maps.append((0, end-body_start, body_start))
        pos = end
        complete = end-body_start == n
    elif (not line.startswith('HTTP/') or
          re.match(r'HTTP/1\.[01] (?:1\d\d|204|304)\b', line)):
        pos = body_start
    else:
        # HTTP/1.1 response with no declared length is close-delimited; a PCAP
        # without verified FIN cannot establish completeness.
        body_parts = [data[body_start:]]
        if len(data) > body_start:
            maps = [(0, len(data)-body_start, body_start)]
        pos = len(data)
        complete = False
        issues.append(f'Close-delimited HTTP response not proven complete {flow}')
    end = min(pos, len(data))
    return {'line': line, 'headers': headers, 'header_bytes': body_start-cursor,
            'body': b''.join(body_parts), 'body_start': body_start, 'maps': maps,
            'chunks': chunks, 'start': cursor, 'end': end,
            'complete': complete, 'start_stamp': stamp(cursor),
            'header_stamp': stamp(cursor), 'body_first_stamp': stamp(maps[0][2]) if maps else (None, None),
            'body_last_stamp': stamp(maps[-1][2]+maps[-1][1]-maps[-1][0]-1) if maps else (None, None),
            'end_stamp': stamp(end-1) if end>cursor else (None, None)}


def _wire_of(maps, offset):
    for a, b, base in maps:
        if a <= offset < b:
            return base+offset-a
    return None


def _sse_events(msg, stamp, issues, flow):
    body = msg['body']
    matches = list(re.finditer(rb'.*?(?:\r?\n\r?\n)', body, re.S))
    residual = body[matches[-1].end() if matches else 0:]
    if residual.strip():
        issues.append(f'Incomplete SSE event {flow}: {len(residual)} trailing decoded bytes')
    call_by_item_id = {}
    name_by_item_id = {}
    for index, match in enumerate(matches):
        block = match.group()
        data_lines = []
        event_type = None
        for line in block.splitlines():
            if line.startswith(b'event:'):
                event_type = line[6:].strip().decode('utf-8', 'replace')
            elif line.startswith(b'data:'):
                part = line[5:]
                data_lines.append(part[1:] if part.startswith(b' ') else part)
        payload = b'\n'.join(data_lines)
        obj = None
        if payload and payload != b'[DONE]':
            try:
                obj = json.loads(payload)
                if not isinstance(obj, dict):
                    obj = None
            except (ValueError, UnicodeDecodeError):
                issues.append(f'Invalid SSE JSON {flow} event={index}')
        if event_type is None and obj:
            event_type = obj.get('type')
        response = obj.get('response') if obj else None
        response = response if isinstance(response, dict) else {}
        item = obj.get('item') if obj else None
        item = item if isinstance(item, dict) else {}
        tool = obj.get('tool_call') if obj else None
        tool = tool if isinstance(tool, dict) else {}
        delta = obj.get('delta') if obj else None
        delta_bytes = len(delta.encode('utf-8')) if isinstance(delta, str) else None
        arguments = (item.get('arguments') or tool.get('arguments') or (obj.get('arguments') if obj else None))
        start_wire = _wire_of(msg['maps'], match.start())
        end_wire = _wire_of(msg['maps'], match.end()-1)
        start_ns, start_fr = stamp(start_wire) if start_wire is not None else (None, None)
        end_ns, end_fr = stamp(end_wire) if end_wire is not None else (None, None)
        framing = None
        # Allocate chunk header/footer only when each participating chunk belongs
        # wholly to THIS SSE event. Otherwise overhead cannot be assigned uniquely.
        touched = [c for c in msg['chunks'] if c[0] < match.end() and c[1] > match.start()]
        if touched and all(c[0] >= match.start() and c[1] <= match.end() for c in touched):
            framing = sum(c[2] for c in touched)
        seq = obj.get('sequence_number') if obj else None
        response_id = (response.get('id') or obj.get('response_id')) if obj else None
        item_id = (obj.get('item_id') or item.get('id')) if obj else None
        explicit_call_id = (obj.get('call_id') or item.get('call_id') or
                            tool.get('call_id') or obj.get('tool_call_id')) if obj else None
        if item_id and explicit_call_id:
            call_by_item_id[item_id] = explicit_call_id
        tool_id = explicit_call_id or call_by_item_id.get(item_id)
        explicit_name = item.get('name') or tool.get('name') or (obj.get('name') if obj else None)
        if item_id and explicit_name:
            name_by_item_id[item_id] = explicit_name
        name = explicit_name or name_by_item_id.get(item_id)
        usage = response.get('usage') if event_type in ('response.completed', 'response.failed', 'response.incomplete') else None
        usage = usage if isinstance(usage, dict) else {}
        details = usage.get('output_tokens_details')
        details = details if isinstance(details, dict) else {}
        input_details = usage.get('input_tokens_details')
        input_details = input_details if isinstance(input_details, dict) else {}
        yield {
            'sse_event_index': index, 'event_type': event_type, 'response_id': response_id,
            'sequence_number': seq, 'tool_call_id': tool_id, 'tool_item_id': item_id, 'tool_call_name': name,
            'tool_event_kind': ('arguments_delta' if event_type and 'arguments.delta' in event_type else
                                'arguments_done' if event_type and 'arguments.done' in event_type else
                                'function_name' if name is not None else None),
            'tool_arguments_bytes': len(arguments.encode('utf-8')) if isinstance(arguments, str) else None,
            'delta_bytes': delta_bytes, 'encoded_bytes': len(block), 'event_bytes': len(block),
            'sse_data_bytes': len(payload), 'http_chunk_framing_bytes': framing,
            'stream_offset_start': start_wire, 'stream_offset_end': end_wire+1 if end_wire is not None else None,
            'event_first_packet_time': start_ns, 'event_last_packet_time': end_ns,
            'frame_start': start_fr, 'frame_end': end_fr, 'frame': end_fr,
            'input_tokens': usage.get('input_tokens', 'unavailable'),
            'output_tokens': usage.get('output_tokens', 'unavailable'),
            'total_tokens': usage.get('total_tokens', 'unavailable'),
            'reasoning_tokens': details.get('reasoning_tokens', 'unavailable'),
            'cached_input_tokens': input_details.get('cached_tokens', 'unavailable'),
            'completion_status': response.get('status', 'unavailable') if event_type and event_type.startswith('response.') else 'unavailable',
            '_observed_ns': end_ns, '_object_type': event_type
        }


def classify_compaction(body):
    """Codex 0.169 observed final handoff template; user role alone is not proof.
    Recognize the precise generated template plus no tools and prior context.
    A full malicious imitation cannot be distinguished from wire data alone.
    """
    try:doc=json.loads(body)
    except (ValueError,TypeError):return 'unknown','invalid_json'
    if not isinstance(doc,dict):return 'unknown','non_object'
    items=doc.get('input')
    if not isinstance(items,list) or not items:return 'unknown','missing_input_array'
    last=items[-1]
    if not isinstance(last,dict):return 'unknown','invalid_last_item'
    parts=last.get('content',[])
    if isinstance(parts,str):parts=[{'text':parts}]
    if not isinstance(parts,list):return 'unknown','invalid_content'
    texts=[p.get('text','') for p in parts if isinstance(p,dict) and isinstance(p.get('text'),str)]
    prefix='You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary for another LLM that will resume the task.'
    required=('Current progress and key decisions made','Important context, constraints, or user preferences','What remains to be done (clear next steps)','Any critical data, examples, or references needed to continue')
    if doc.get('tools')==[] and last.get('role') in ('user','developer','system') and len(items)>1 and any(isinstance(i,dict) and i.get('role')=='developer' for i in items[:-1]) and any(t.startswith(prefix) and all(x in t for x in required) for t in texts):
        return 'compaction','Codex 0.169 exact final handoff template + tools=[] + prior developer context (wire signature)'
    if any(t.lstrip().startswith('CONTEXT CHECKPOINT COMPACTION') or t.startswith(prefix) for t in texts):
        return 'unknown','checkpoint-like final item without verified full signature'
    return 'normal','no verified checkpoint signature in final instruction'

def _metric_samples(p, event, summary):
    f = p/'metrics_samples.jsonl'
    if not f.exists():
        summary['metrics_sample_count'] = 0
        summary['metrics_status'] = 'disabled_or_unavailable'
        return
    seen, errors, metric_names = 0, 0, set()
    resets=0;restarts=0;families={}
    for line_no,line in enumerate(f.read_text().splitlines(),1):
        try:
            sample = json.loads(line)
            if not isinstance(sample, dict):
                raise ValueError('nonobject')
            metric_names.update(sample.get('metric_names', []))
            resets+=len(sample.get('counter_resets',[]))
            restarts+=bool(sample.get('service_process_changed'))
            families.update(sample.get('families',{}).get('types',{}))
            event('vllm_metrics_sample', sample.get('observed_ns'), source='vllm_prometheus_scrape',
                  timestamp_semantics='application-generated scrape completion; aggregate metrics, not per-request',
                  measurement_control=True, model_request_id=None, metrics=sample.get('metrics', []),
                  raw_sample=sample, source_record_file='metrics_samples.jsonl',source_record_line=line_no,
                  scrape_start_ns=sample.get('scrape_start_ns'), scrape_end_ns=sample.get('observed_ns'),
                  sampling_interval_seconds=sample.get('interval_seconds'), scrape_error=sample.get('error'))
            seen += 1
            errors += bool(sample.get('error'))
        except (ValueError, KeyError, TypeError):
            errors += 1
    summary.update(metrics_sample_count=seen, metrics_scrape_failures=errors,
                   metrics_names_observed=sorted(metric_names),
                   metrics_status='sampled_aggregate_not_request_exact' if seen else 'no_samples',metrics_counter_resets=resets,metrics_service_restarts=restarts,metrics_family_types_observed=families)



def _merge_v3_sources(p, events, summary):
    """Join by explicit Responses IDs, never by nearest timestamp.

    The packet parser's deterministic model_request_id is distinct from the
    engine's request_id. This join is unambiguous only for a unique response.id.
    TCP/SSH samples deliberately stay connection-level and unlinked.
    """
    p=pathlib.Path(p)
    # Response IDs are present on decoded stream events, not x-client-request-id.
    candidates=collections.defaultdict(set)
    for e in events:
        if e.get('type')=='sse_event_observed' and e.get('response_id') and e.get('model_request_id'):
            candidates[e['response_id']].add(e['model_request_id'])
    # A middleware-generated span links early body-read events to the request ID
    # discovered later at response.headers, without timing-based guessing.
    spans={}
    pending=[]
    v3=p/'v3_events.jsonl'
    if v3.is_file():
        for line_no,line in enumerate(v3.read_text().splitlines(),1):
            try:
                record=json.loads(line)
                if isinstance(record,dict):
                    pending.append((line_no,record))
                    if record.get('asgi_span_id') and record.get('request_id'):
                        spans.setdefault(record['asgi_span_id'],set()).add(record['request_id'])
            except ValueError:summary.setdefault('v3_parse_errors',[]).append(line_no)
    matched=unmatched=ambiguous=0
    bysource=collections.Counter()
    if v3.is_file():
        for n,e in pending:
            if not isinstance(e.get('timestamp_ns'),int):
                summary.setdefault('v3_parse_errors',[]).append(n)
            span_ids=spans.get(e.get('asgi_span_id'),set())
            raw_response_id=e.get('response_id') or e.get('request_id') or (next(iter(span_ids)) if len(span_ids)==1 else None)
            response_id=parent_response_id(raw_response_id)
            choices=candidates.get(response_id,set()) if response_id else set()
            if len(choices)==1 and len(span_ids)<=1:
                outcome='matched';rid=next(iter(choices));matched+=1
            elif len(choices)>1 or len(span_ids)>1:
                outcome='ambiguous';rid=None;ambiguous+=1
            else:
                outcome='unmatched';rid=None;unmatched+=1
            source=e.get('source','unknown')
            bysource[source]+=1
            event={**e,'type':e.get('event_type','unknown'),
                   'observed_ns':e.get('timestamp_ns'),
                   'timestamp_semantics':e.get('timestamp_semantics','source call-site observation'),
                   'run_id':summary.get('run_id'), 'task_id':summary.get('task_id'),
                   'model_request_id':rid,'link_status':outcome,
                   'correlation_status':outcome,'parent_response_id':response_id,
                   'original_request_id':e.get('request_id'),
                   'link_evidence':'deterministic vLLM child -> parent response ID; unique SSE response.id',
                   'source_record_file':'v3_events.jsonl','source_record_line':n, 'source_record_sha256':hashlib.sha256(json.dumps(e,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),
                   'source_category':source}
            events.append(event)
    # NOTE: Original network events retain their legacy schema fields unmodified.
    for e in events:
        e.setdefault('schema_version','agent-traffic-v3.0' if e.get('source_category') else 'agent-traffic-http-sse-v2.1')
        e.setdefault('event_type',e.get('type'))
        e.setdefault('timestamp_ns',e.get('observed_ns'))
    # TCP diagnostics do not belong to any particular vLLM request.
    tcp=p/'tcp_samples.jsonl';tcp_count=0;tcp_failures=0
    if tcp.is_file():
        for line_no,line in enumerate(tcp.read_text().splitlines(),1):
            try:
                e=json.loads(line)
                if not isinstance(e,dict):raise ValueError('Not a dict')
            except (ValueError,TypeError):tcp_failures+=1;continue
            events.append({'schema_version':'agent-traffic-v3.0','run_id':summary.get('run_id'),
                'task_id':summary.get('task_id'),'source':'tcp_info','type':'tcp_host_ssh_snapshot',
                'event_type':'tcp_host_ssh_snapshot','observed_ns':e.get('observed_ns'),
                'timestamp_ns':e.get('observed_ns'),'clock':'Spark CLOCK_REALTIME',
                'timestamp_semantics':'ss snapshot, multiple SSH flows; not a per-request timestamp',
                'model_request_id':None,'request_id':None,'link_status':'unmatched_connection_level',
                'measurement_control':True,'tcp_snapshot':e,
                'source_record_file':'tcp_samples.jsonl','source_record_line':line_no,'raw_sample':e,
                'correlation_status':'not_applicable'});tcp_count+=1;tcp_failures+=bool(e.get('error'))
    telemetry_file=p/'v3_telemetry_collection.json'
    if telemetry_file.is_file():
        try:summary['v3_raw_capture']=json.loads(telemetry_file.read_text())
        except ValueError:summary['v3_raw_capture']={'error':'invalid telemetry collection JSON'}
    summary.update(v3_event_count=sum(bysource.values()),v3_events_by_source=dict(bysource),
                   v3_explicit_id_matches=matched,v3_unmatched=unmatched,
                   v3_ambiguous=ambiguous,v3_join_method='exact Responses ID to PCAP SSE response.id only',
                   tcp_snapshot_count=tcp_count,tcp_parse_failures=tcp_failures,
                   v3_event_status='not_enabled_or_unavailable' if not v3.exists() else 'observed_unverified_in_live_engine',
                   per_request_timing_accuracy='not inferred from Prometheus or PCAP alone')


def _match_transactions(requests, responses):
    """Ordered HTTP/1.1 matching, with at most one final response per request.

    Informational (1xx except 101) responses do not consume a request.
    An orphan final response is never assigned to the previous request.
    As with any passive HTTP/1.1 trace, missing prefix data that leaves equal
    counts can be impossible to distinguish without external application IDs.
    """
    result = []
    final_index = 0
    for resp in responses:
        try:
            status = int(resp['line'].split()[1])
        except (ValueError, IndexError):
            status = None
        if status is not None and 100 <= status < 200 and status != 101:
            req = requests[final_index] if final_index < len(requests) else None
            result.append((req, resp, 'informational' if req is not None else 'ambiguous'))
            continue
        # A missing request cannot be inferred from the cardinality of the
        # opposite direction. Never reuse the last request for an extra reply.
        req = requests[final_index] if final_index < len(requests) else None
        result.append((req, resp, 'matched' if req is not None else 'unmatched'))
        final_index += 1
    for req in requests[final_index:]:
        result.append((req, None, 'unmatched'))
    return result


def _transaction_quality(pairs, issues, sse_checks, canonical_interface):
    finals = [(req, resp, status) for req, resp, status in pairs if status != 'informational']
    matched = sum(status == 'matched' for _, _, status in finals)
    ambiguous = sum(status == 'ambiguous' for _, _, status in finals)
    unmatched_requests = sum(req is not None and status != 'matched' for req, _, status in finals)
    unmatched_responses = sum(resp is not None and status != 'matched' for _, resp, status in finals)
    nreq = sum(req is not None for req, _, _ in finals)
    nresp = sum(resp is not None for _, resp, _ in finals)
    seen_request = set()
    seen_response = set()
    for req, resp, status in finals:
        if status != 'matched':
            continue
        # Object identity, not client-request-id: final response must be bijective.
        if id(req) in seen_request or id(resp) in seen_response:
            matched -= 1
            ambiguous += 1
        seen_request.add(id(req))
        seen_response.add(id(resp))
    http_complete = bool(nreq and nresp) and all(
        m['complete'] for pair in finals for m in pair[:2] if m is not None) and not any(
        ('TCP gap' in issue or 'HTTP ' in issue or 'chunk' in issue.lower()) for issue in issues)
    association_complete = (matched == nreq == nresp and unmatched_requests == 0 and
                            unmatched_responses == 0 and ambiguous == 0)
    sse_complete = all(sse_checks) and not any('SSE ' in issue for issue in issues)
    return {'canonical_interface': canonical_interface,
            'http_message_complete': http_complete,
            'http_request_response_match_complete': association_complete,
            'sse_reconstruction_complete': sse_complete,
            'matched': max(0, matched), 'unmatched': unmatched_requests + unmatched_responses,
            'ambiguous': ambiguous, 'unmatched_requests': unmatched_requests,
            'unmatched_responses': unmatched_responses,
            'final_response_count': nresp, 'request_count': nreq,
            'true_match_rate': (max(0, matched)/nresp if nresp else None),
            'match_rate_denominator': 'final HTTP responses (not informational 1xx)',
            'quality_complete': http_complete and association_complete and sse_complete}


def parse(p):
    p = pathlib.Path(p)
    state = json.loads((p/'state.json').read_text())
    pcap = p/'server_traffic.pcapng'
    events, issues = [], []

    def event(kind, ns=None, **kw):
        src = kw.pop('source', 'server_pcapng' if kind not in ('scope', 'capture_end') else 'spark_capture_state')
        semantics = kw.pop('timestamp_semantics', 'network-observed packet capture' if src == 'server_pcapng' else 'application-generated controller timestamp')
        events.append({'run_id': state['run_id'], 'task_id': state['task_id'], 'type': kind,
                       'observed_ns': ns, 'clock': 'Spark CLOCK_REALTIME packet timestamp' if src == 'server_pcapng' and ns is not None else None,
                       'source': src, 'timestamp_semantics': semantics,
                       'model_request_id': kw.pop('model_request_id', None), **kw})

    event('scope', state.get('ready_ns'), interfaces=state.get('interfaces'),
          analysis_interface=state.get('analysis_interface'),
          canonical_interface='docker0',
          parser_version=PARSER_VERSION, unavailable='computed_from_observed_events',
          duplicate_policy='Bridge only for HTTP byte accounting; never add loopback/veth/physical observations')
    cmd = ['tshark', '-n', '-r', str(pcap), '-T', 'fields']
    for field in FIELDS:
        cmd.extend(['-e', field])
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if r.returncode:
        raise RuntimeError('PCAPNG could not be decoded: '+r.stderr[-3000:])
    counts, streams = collections.Counter(), collections.defaultdict(list)
    first = last = None
    observations, duplicates = {}, 0
    for row in r.stdout.splitlines():
        val = row.split('\t')
        val += ['']*(len(FIELDS)-len(val))
        ts, iface, src, sp, dst, dp, seq, payload, frame, tcp_stream, syn, fin, reset = val[:len(FIELDS)]
        if not ts:
            continue
        try:
            ns = epoch_ns(ts)
        except Exception:
            issues.append(f'Invalid timestamp in frame {frame}')
            continue
        first = ns if first is None else min(first, ns)
        last = ns if last is None else max(last, ns)
        counts[iface] += 1
        if not (payload and (sp == '8000' or dp == '8000')):
            continue
        try:
            fr = int(frame)
            raw = bytes.fromhex(payload.replace(':', ''))
            sequence = int(seq)
        except (ValueError, TypeError):
            issues.append(f'Invalid TCP payload frame {frame}')
            continue
        signature = (src, sp, dst, dp, sequence, hashlib.sha256(raw).digest())
        previous = observations.get(signature)
        if previous and previous[1] != iface and 0 <= ns-previous[0] <= 5_000_000:
            event('duplicate_packet_observation', ns, frame=fr, interface=iface,
                  reference_frame=previous[2], reference_interface=previous[1],
                  rule='same TCP flow/sequence/payload observed on another interface')
            duplicates += 1
        else:
            observations[signature] = (ns, iface, fr)
        if iface == 'docker0':
            # tcp.stream is a frame-stable capture ordinal; unlike a four-tuple,
            # it disambiguates later connections reusing the same ephemeral port.
            conn_key = tcp_stream if tcp_stream else '|'.join(sorted([f'{src}:{sp}', f'{dst}:{dp}']))
            direction = (src, sp, dst, dp)
            streams[(conn_key, direction)].append((sequence, raw, ns, fr))
    connections = collections.defaultdict(lambda: {'requests': [], 'responses': []})
    connection_ids = {}
    for (conn_key, flow), segments in streams.items():
        segments.sort(key=lambda x: (x[0], x[2], x[3]))
        end_seq, buf, spans = segments[0][0], bytearray(), []
        for seq, payload, ns, frame in segments:
            if seq > end_seq:
                issues.append(f'TCP gap in {flow} stream {conn_key}: {seq-end_seq} bytes')
                break
            skip = max(0, end_seq-seq)
            if skip >= len(payload):
                continue
            piece = payload[skip:]
            begin = len(buf)
            buf.extend(piece)
            spans.append((begin, len(buf), ns, frame))
            end_seq = seq+len(payload)
        offsets = [x[0] for x in spans]

        # Capture flow-local span tables now: SSE decoding happens *after*
        # scanning all directions and must not close over the final flow.
        def stamp(off, _offsets=offsets, _spans=spans):
            if off is None or off < 0 or not _spans:
                return (None, None)
            i = bisect.bisect_right(_offsets, off)-1
            if i >= 0 and off < _spans[i][1]:
                return (_spans[i][2], _spans[i][3])
            return (None, None)

        endpoint = '|'.join(sorted([f'{flow[0]}:{flow[1]}', f'{flow[2]}:{flow[3]}']))
        cid = f'{endpoint}#tcp-stream-{conn_key}' if str(conn_key).isdigit() else endpoint
        connection_ids[conn_key] = cid
        cursor, index = 0, 0
        while cursor < len(buf):
            msg = _message(bytes(buf), cursor, stamp, issues, flow)
            if not msg:
                break
            msg.update(flow=list(flow), connection_id=cid, message_index=index,
                       tcp_seq_raw_start=segments[0][0]+cursor,
                       stream_offset_base=cursor, stamp=stamp)
            kind = 'responses' if msg['line'].startswith('HTTP/') else 'requests'
            connections[conn_key][kind].append(msg)
            if not msg['complete']:
                issues.append(f'Incomplete HTTP message {flow}: {msg["line"]}')
                break
            if msg['end'] <= cursor:
                issues.append(f'HTTP parser did not advance {flow}')
                break
            cursor = msg['end']
            index += 1
    nrequest = nresponse = nsse = nmodel = 0
    byte_totals = collections.Counter()
    token_totals = collections.Counter()
    token_coverage = 0
    event_types = collections.Counter()
    transaction_pairs = []
    sse_checks = []
    for conn_key, paired in connections.items():
        reqs, resps = paired['requests'], paired['responses']
        # Ordinary HTTP/1.1: responses are ordered on a connection, including
        # keepalive and pipelining. 1xx informational replies do not consume a request.
        tx = -1
        request_pairs = []
        for req in reqs:
            tx += 1
            identity = f'{state["run_id"]}|{state["task_id"]}|{req["connection_id"]}|{tx}'
            rid = 'mr_'+hashlib.sha256(identity.encode('utf-8')).hexdigest()[:24]
            request_pairs.append((rid, req))
            req['model_request_id'] = rid
            req['http_transaction_index'] = tx
        pairs = _match_transactions(reqs, resps)
        transaction_pairs.extend(pairs)
        # Reassociate all messages by actual ordered final transactions; do not
        # silently recycle a request id for an orphan response.
        for req, resp, status in pairs:
            if resp is None:
                issues.append(f'Unmatched HTTP request on {req["connection_id"]}')
                continue
            if status in ('matched', 'informational'):
                resp['model_request_id'] = req['model_request_id']
                resp['http_transaction_index'] = req['http_transaction_index']
            else:
                resp['model_request_id'] = None
                resp['http_transaction_index'] = None
                issues.append(f'{status.title()} HTTP response on {resp["connection_id"]}')
        if len(reqs) != sum(status == 'matched' for _, _, status in pairs):
            issues.append(f'HTTP transaction mismatch {connection_ids.get(conn_key)}')
        # Discover response.id from SSE before emitting HTTP events to populate
        # BOTH request and response records, independent of event ordering.
        resp_ids = {}
        sse_records = {}
        for resp in resps:
            if 'text/event-stream' not in resp['headers'].get('content-type', ''):
                continue
            parsed = list(_sse_events(resp, resp['stamp'], issues, resp['flow']))
            sse_records[id(resp)] = parsed
            linked_request = next((req for req, rr, status in pairs if rr is resp and status == 'matched'), None)
            is_model = linked_request is not None and linked_request['line'].split()[1].startswith('/v1/responses')
            terminal = any(se['event_type'] in ('response.completed', 'response.failed', 'response.incomplete') for se in parsed)
            sse_checks.append(resp['complete'] and bool(parsed) and
                              all(se['event_first_packet_time'] is not None and
                                  se['event_last_packet_time'] is not None for se in parsed) and
                              (not is_model or terminal))
            if is_model and not terminal:
                issues.append(f'Incomplete SSE response without terminal event {resp["connection_id"]}')
            for se in parsed:
                if se['response_id']:
                    resp_ids[resp['model_request_id']] = se['response_id']
                    break
        for typ, msgs in (('request', reqs), ('response', resps)):
            for msg in msgs:
                rid = msg.get('model_request_id')
                common = {'flow': msg['flow'], 'connection_id': msg['connection_id'],
                          'message_index': msg['message_index'],
                          'http_transaction_index': msg['http_transaction_index'],
                          'first_frame': msg['start_stamp'][1], 'last_frame': msg['end_stamp'][1],
                          'http_start_line': msg['line'], 'headers_bytes': msg['header_bytes'],
                          'body_bytes': len(msg['body']),
                          'http_wire_bytes': msg['end']-msg['start'],
                          'byte_accounting': 'Count end event only; start repeats reconstructed attributes',
                          'message_complete': msg['complete'],
                          'completion_observed_ns': msg['end_stamp'][0],
                          'request_id': msg['headers'].get('x-request-id'),
                          'x_client_request_id': msg['headers'].get('x-client-request-id'),
                          'content_type': msg['headers'].get('content-type'),
                          'response_id': resp_ids.get(rid),
                          'stream_offset_start': msg['start'], 'stream_offset_end': msg['end'],
                          'tcp_seq_raw_start': msg['tcp_seq_raw_start'],
                          'model_request_id': rid}
                if typ == 'request':
                    nrequest += 1
                    parts = msg['line'].split(' ')
                    method, path = parts[:2]
                    is_model = method=='POST' and path.split('?')[0] in ('/v1/responses','/v1/responses/compact')
                    nmodel += bool(is_model)
                    if is_model:
                        category,evidence=classify_compaction(msg['body']) if msg['complete'] else ('unknown','incomplete HTTP request')
                        common.update(request_category=category,classification_evidence=evidence,request_body_sha256=hashlib.sha256(msg['body']).hexdigest())
                    control = path.startswith('/metrics') or 'measurement_marker=' in path or path.startswith('/health')
                    byte_totals['http_request_headers_bytes'] += msg['header_bytes']
                    byte_totals['http_request_body_bytes'] += len(msg['body'])
                    common.update(method=method, path=path, direction='client_to_model', measurement_control=control,
                                  request_first_packet_time=msg['start_stamp'][0],
                                  request_body_complete_observed_time=(msg['end_stamp'][0] if msg['complete'] and
                                    'chunked' in msg['headers'].get('transfer-encoding','').lower() else
                                    (msg['body_last_stamp'][0] if msg['body'] else msg['end_stamp'][0])
                                    if msg['complete'] else None),
                                  request_headers_bytes=msg['header_bytes'], request_body_bytes=len(msg['body']),
                                  request_first_frame=msg['start_stamp'][1],
                                  request_first_stream_offset=msg['start'],
                                  request_body_complete_frame=(msg['end_stamp'][1] if msg['complete'] and
                                    'chunked' in msg['headers'].get('transfer-encoding','').lower() else
                                    (msg['body_last_stamp'][1] if msg['body'] else msg['end_stamp'][1])
                                    if msg['complete'] else None),
                                  request_body_complete_stream_offset=(msg['end'] if msg['complete'] and
                                    'chunked' in msg['headers'].get('transfer-encoding','').lower() else
                                    (_wire_of(msg['maps'],len(msg['body'])-1)+1 if msg['body'] else msg['end'])
                                    if msg['complete'] else None),
                                  request_complete=msg['complete'])
                else:
                    nresponse += 1
                    status = int(msg['line'].split()[1])
                    byte_totals['http_response_headers_bytes'] += msg['header_bytes']
                    byte_totals['http_response_body_bytes'] += len(msg['body'])
                    common.update(status=status, direction='model_to_client', response_header_first_packet_time=msg['start_stamp'][0],
                                  response_first_body_packet_time=msg['body_first_stamp'][0],
                                  response_body_complete_observed_time=(msg['end_stamp'][0] if msg['complete'] and
                                    'chunked' in msg['headers'].get('transfer-encoding','').lower() else
                                    msg['body_last_stamp'][0] if msg['body'] and msg['complete'] else None),
                                  response_last_body_data_packet_time=msg['body_last_stamp'][0],
                                  response_headers_bytes=msg['header_bytes'], response_body_bytes=len(msg['body']),
                                  response_header_first_frame=msg['start_stamp'][1],
                                  response_header_first_stream_offset=msg['start'],
                                  response_first_body_frame=msg['body_first_stamp'][1],
                                  response_first_body_stream_offset=(msg['maps'][0][2] if msg['maps'] else None),
                                  response_body_complete_frame=(msg['end_stamp'][1] if msg['complete'] and
                                    'chunked' in msg['headers'].get('transfer-encoding','').lower() else
                                    msg['body_last_stamp'][1] if msg['body'] and msg['complete'] else None),
                                  response_body_complete_stream_offset=(msg['end'] if msg['complete'] and
                                    'chunked' in msg['headers'].get('transfer-encoding','').lower() else
                                    (_wire_of(msg['maps'],len(msg['body'])-1)+1 if msg['body'] and msg['complete'] else None)),
                                  response_complete=msg['complete'],
                                  measurement_control=(rid is None or
                                                       next((x['line'].split()[1].startswith('/metrics') or 'measurement_marker=' in x['line'] or x['line'].split()[1].startswith('/health')
                                                             for rr,x in request_pairs if rr == rid), False)))
                event(f'http_{typ}_start', msg['start_stamp'][0], **common)
                event(f'http_{typ}_end', msg['end_stamp'][0], **common)
                if typ == 'response' and common['status'] >= 400:
                    event('http_error_observed', msg['end_stamp'][0], status=common['status'],
                          flow=msg['flow'], message_index=msg['message_index'], model_request_id=rid)
                if typ == 'response':
                    for se in sse_records.get(id(msg), []):
                        ts = se.pop('_observed_ns')
                        se.pop('_object_type', None)
                        event_types[se['event_type'] or 'unavailable'] += 1
                        if se['event_type'] == 'response.completed':
                            if isinstance(se['input_tokens'], int) and isinstance(se['output_tokens'], int):
                                token_coverage += 1
                                token_totals['input_tokens'] += se['input_tokens']
                                token_totals['output_tokens'] += se['output_tokens']
                                if isinstance(se['total_tokens'], int):
                                    token_totals['total_tokens'] += se['total_tokens']
                                if isinstance(se['reasoning_tokens'], int):
                                    token_totals['reasoning_tokens'] += se['reasoning_tokens']
                        byte_totals['sse_encoded_bytes'] += se['encoded_bytes']
                        if isinstance(se['delta_bytes'],int):byte_totals['sse_delta_bytes'] += se['delta_bytes']
                        if isinstance(se['http_chunk_framing_bytes'],int):
                            byte_totals['sse_assignable_chunk_framing_bytes'] += se['http_chunk_framing_bytes']
                        event('sse_event_observed', ts, model_request_id=rid, flow=msg['flow'], direction='model_to_client',
                              connection_id=msg['connection_id'], http_transaction_index=msg['http_transaction_index'],
                              response_id=se['response_id'] or resp_ids.get(rid), **{k:v for k,v in se.items() if k!='response_id'})
                        nsse += 1
    # IAT is measured only for model requests, excluding health/metrics probes;
    # it is an observed bridge first-payload-byte interval, NOT API arrival time.
    model_starts = sorted((e for e in events if e['type']=='http_request_start' and
                           e.get('path','').startswith('/v1/responses') and e['observed_ns'] is not None),
                          key=lambda e:e['observed_ns'])
    for index,e in enumerate(model_starts):
        e['model_request_interarrival_ns'] = (e['observed_ns']-model_starts[index-1]['observed_ns']
                                                if index else None)
    quality = _transaction_quality(transaction_pairs, issues, sse_checks, 'docker0')
    for issue in issues:
        event('reassembly_error', None, error=issue, timestamp_semantics='parser diagnostic; no exact packet timestamp')
    event('capture_end', state.get('ended_ns'), state=state.get('state'),
          stop_reason=state.get('stop_reason'), error=state.get('error'))
    log = (p/'capture.log').read_text(errors='replace') if (p/'capture.log').exists() else ''
    captured = re.search(r'Packets captured:\s*(\d+)', log)
    drops = [int(x) for x in re.findall(r"Packets received/dropped on interface '[^']+':\s*\d+/(\d+)", log)]
    summary = {'parser_version': PARSER_VERSION, 'packet_count': sum(counts.values()),
               'packets_by_interface': dict(counts),
               'capture_reported_packets': int(captured.group(1)) if captured else None,
               'packet_count_matches': bool(captured) and int(captured.group(1)) == sum(counts.values()),
               'dropped_packets': sum(drops) if drops else None,
               'zero_loss_verified': bool(drops) and not any(drops),
               'pcapng_readable': True, 'first_packet_ns': first, 'last_packet_ns': last,
               'http_requests': nrequest, 'agent_responses_requests': nmodel,
               'http_responses': nresponse, 'sse_events': nsse,
               'sse_event_types': dict(event_types), 'completed_usage_count': token_coverage,
               'completed_usage_totals': dict(token_totals),
               'byte_totals_canonical_bridge': dict(byte_totals),
               'sse_encoding_amplification_ratio': (byte_totals['sse_encoded_bytes']/byte_totals['sse_delta_bytes']
                   if byte_totals['sse_delta_bytes'] else None),
               'sse_amplification_semantics': 'All SSE data+envelopes / only event delta UTF-8 bytes; excludes HTTP chunk framing and includes non-delta events',
               'reassembly_issues': issues, 'http_timeline_complete': quality['quality_complete'],
               'data_quality': quality,
               'observation_point': 'docker0', 'canonical_interface': 'docker0',
               'cross_interface_duplicate_accounting': 'bridge only, never sum across interfaces',
               'application_timepoints': 'unavailable; packet observations only',
               'duplicate_observations_marked': duplicates,
               'duplicate_rule': 'Diagnostic heuristic only: cross-interface observations are not globally deduplicated or additive',
               'metric_coverage': {'http_request_ids': nrequest,
                                   'sse_events_with_type': sum(v for k,v in event_types.items() if k!='unavailable'),
                                   'sse_events_total': nsse, 'completed_usage': token_coverage,
                                   'response_matched_to_request': quality['matched'],
                                   'unmatched_responses': quality['unmatched_responses'],
                                   'unmatched_requests': quality['unmatched_requests'],
                                   'ambiguous_transactions': quality['ambiguous'],
                                   'typed_sse_fraction': (sum(v for k,v in event_types.items() if k!='unavailable')/nsse if nsse else None),
                                   'completed_usage_per_model_request_fraction': (token_coverage/nmodel if nmodel else None),
                                   'linked_responses_fraction': quality['true_match_rate']}}
    _metric_samples(p, event, summary)
    summary['run_id']=state['run_id'];summary['task_id']=state['task_id']
    _merge_v3_sources(p, events, summary)
    summary['model_request_classification']=classification_summary(events)
    # Source sample timing is a service/connection observation, never a
    # per-request timestamp. Existing raw JSONL remains on Spark.
    sampling={}
    for label,filename in (('prometheus','metrics_samples.jsonl'),('tcp','tcp_samples.jsonl')):
        records=[]
        try:
            with (p/filename).open() as fh:
                records=[json.loads(line) for line in fh if line.strip()]
        except FileNotFoundError:pass
        interval=state.get('metrics_interval_seconds') if label=='prometheus' else state.get('tcp_interval_seconds')
        stats=sample_statistics(records,configured_interval=interval)
        stats['missed_cycles']=state.get('sampling_missed_cycles',{}).get(label)
        sampling[label]=stats
    summary['sampling']=sampling
    summary['correlation']={'method':'explicit Responses ID and deterministic child-parent mapping',
       'matched':summary.get('v3_explicit_id_matches',0),
       'unmatched':summary.get('v3_unmatched',0),'ambiguous':summary.get('v3_ambiguous',0)}
    summary['availability']=availability(events,state.get('detailed_events'),bool(summary.get('v3_parse_errors') or state.get('telemetry_collection_error')))
    summary['unavailable']=sorted(NEVER)
    summary['telemetry_integrity']={'parse_errors':summary.get('v3_parse_errors',[]),'metrics_failures':summary.get('metrics_scrape_failures',0),'tcp_failures':summary.get('tcp_parse_failures',0),'collection_error':state.get('telemetry_collection_error'),'writer_snapshot_consistency':'periodic, not task-end barrier'}
    summary['application_timepoints']='see availability; call-site observations, not GPU/kernel timestamps'
    for e in events:
        e.update(normalize_event(e))
        if e.get('type')=='scope':e['unavailable']=sorted(NEVER)
    events.sort(key=lambda e: (e['observed_ns'] is None, e['observed_ns'] or 0,
                               e['type'], e.get('model_request_id') or ''))
    atomic_jsonl(p/'server_timeline.jsonl',events)
    (p/'analysis.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary


if __name__ == '__main__':
    print(json.dumps(parse(pathlib.Path(sys.argv[1]))))
