"""Opt-in low-cost event telemetry for the pinned vLLM 0.24.0 overlay.

Every timestamp describes *this Python observation site*, not a GPU completion,
TCP socket write, NIC transmission, or remote ACK. No token text is persisted.
"""
from __future__ import annotations
import atexit
import collections
import json
import os
from pathlib import Path
import queue
import threading
import time
import uuid

SCHEMA = 'agent-traffic-v3.0'
_ENABLED = os.getenv('AGENT_TRAFFIC_V3_EVENTS', '0') == '1'
_LOG_ROOT = Path(os.getenv('AGENT_TRAFFIC_V3_DIR', '/runtime/v3_telemetry'))

class BoundedEventWriter:
    def __init__(self, root, max_queue=8192, max_bytes=64 * 1024 * 1024):
        self.root=Path(root); self.max_bytes=max_bytes
        self.queue=queue.Queue(maxsize=max_queue)
        self.counts=collections.Counter();self.lock=threading.Lock();self.closed=False
        self.root.mkdir(parents=True, exist_ok=True)
        self.path=self.root / f'events-{os.getpid()}-NNN.jsonl'
        self.stats_path=self.root / f'writer-{os.getpid()}.json'
        self.thread=threading.Thread(target=self._drain, daemon=True, name='agent-traffic-v3-writer')
        self.thread.start()
    def emit(self, data):
        if self.closed:return
        try:self.queue.put_nowait(data)
        except queue.Full:
            with self.lock:self.counts['dropped_queue_full']+=1
    def _drain(self):
        part=0;total=0;max_parts=16;last_stats=time.monotonic()
        stream=None
        try:
            while True:
                try:record=self.queue.get(timeout=.25)
                except queue.Empty:
                    if stream is not None:stream.flush()
                    if self.closed:break
                    if time.monotonic()-last_stats>5:
                        self._stats();last_stats=time.monotonic()
                    continue
                if record is None:break
                line=json.dumps(record,ensure_ascii=False,separators=(',',':'))+'\n'
                encoded_size=len(line.encode('utf-8'))
                if total+encoded_size > self.max_bytes*max_parts:
                    with self.lock:self.counts['dropped_log_limit']+=1
                    self.queue.task_done()
                    continue
                if stream is None or stream.tell()+encoded_size>self.max_bytes:
                    if stream is not None:stream.close();part+=1
                    path=self.root/f'events-{os.getpid()}-{part:03d}.jsonl'
                    stream=path.open('a', buffering=1024*64)
                    with self.lock:self.counts['files_opened']+=1
                try:stream.write(line)
                except OSError:
                    with self.lock:self.counts['write_errors']+=1
                else:
                    total+=encoded_size
                    with self.lock:self.counts['written']+=1;self.counts['written_bytes']=total
                finally:self.queue.task_done()
                if self.queue.empty():stream.flush()
                if time.monotonic()-last_stats>5:
                    self._stats();last_stats=time.monotonic()
        finally:
            if stream is not None:stream.close()
            self._stats()
    def _stats(self):
        with self.lock:counts=dict(self.counts)
        try:self.stats_path.write_text(json.dumps({'pid':os.getpid(),'schema_version':SCHEMA,
                        'queue_capacity':self.queue.maxsize,'path':str(self.path),
                        'counts':counts,'closed':self.closed,'clock':'CLOCK_REALTIME + monotonic'}, indent=2))
        except OSError:pass
    def close(self):
        if self.closed:return
        self.closed=True
        try:self.queue.put_nowait(None)
        except queue.Full:pass
        self.thread.join(timeout=2)
        self._stats()

_WRITER=None
_WRITER_PID=None

def enabled():return _ENABLED

def emit(source,event_type,**fields):
    """Nonblocking; records two clocks on the caller thread."""
    global _WRITER,_WRITER_PID
    if not _ENABLED:return
    if _WRITER_PID != os.getpid():
        _WRITER=BoundedEventWriter(_LOG_ROOT)
        _WRITER_PID=os.getpid()
        atexit.register(_WRITER.close)
    record={'schema_version':SCHEMA,'source':source,'event_type':event_type,
            'timestamp_ns':time.time_ns(),'monotonic_ns':time.monotonic_ns(),
            'clock':'CLOCK_REALTIME','timestamp_semantics':'call-site observation',
            'pid':os.getpid(),**fields}
    _WRITER.emit(record)


def event_meta(obj):
    """Extract only public routing metadata, never model text or tool arguments."""
    response=getattr(obj,'response',None)
    return {'sse_event_type':str(getattr(obj,'type','unknown')),
            'response_id':getattr(response,'id',None) or getattr(obj,'response_id',None),
            'sequence_number':getattr(obj,'sequence_number',None),
            'tool_call_id':getattr(obj,'call_id',None),
            'delta_bytes':len(str(getattr(obj,'delta','')).encode('utf-8')) if hasattr(obj,'delta') else None}


class AgentTrafficMiddleware:
    """ASGI receive/send boundary instrumented without buffering request body.

    ASGI send completion is NOT socket write completion. Stream offsets are ASGI
    body byte offsets and must be joined with PCAP offsets only after HTTP
    chunk framing is handled. /metrics and /health are measurement-control.
    """
    def __init__(self,app):self.app=app
    async def __call__(self,scope,receive,send):
        if not _ENABLED or scope['type']!='http':
            return await self.app(scope,receive,send)
        path=scope.get('path','')
        control=path.startswith(('/health','/metrics'))
        if not (path.startswith('/v1/responses') or control):
            return await self.app(scope,receive,send)
        source='measurement_control' if control else 'api_server'
        client=scope.get('client');server=scope.get('server')
        connection_id='-'.join(map(str,client or ()))+'->'+'-'.join(map(str,server or ()))
        common={'asgi_span_id':uuid.uuid4().hex,'connection_id':connection_id,'client':list(client) if client else None,
                'server':list(server) if server else None,'path':path,'method':scope.get('method'),
                'measurement_control':control}
        emit(source,'request_receive_start',**common)
        body_total=0;body_end=False;response_total=0;chunks=0
        async def tracked_receive():
            nonlocal body_total,body_end
            msg=await receive()
            if msg['type']=='http.request':
                body_total+=len(msg.get('body',b''))
                if not msg.get('more_body') and not body_end:
                    body_end=True
                    emit(source,'request_body_complete',request_body_bytes=body_total,**common)
            elif msg['type']=='http.disconnect':
                emit(source,'http_disconnect',**common)
            return msg
        async def tracked_send(msg):
            nonlocal response_total,chunks
            state=scope.get('state',{})
            metadata=state.get('request_metadata') if isinstance(state,dict) else None
            request_id=getattr(metadata,'request_id',None)
            if msg['type']=='http.response.start':
                emit(source,'response_headers_ready',request_id=request_id,
                     status=msg.get('status'),**common)
            elif msg['type']=='http.response.body':
                n=len(msg.get('body',b''));offset=response_total;response_total+=n
                emit(source,'asgi_send_start',request_id=request_id,
                     body_chunk_index=chunks,body_bytes=n,stream_offset=offset,**common)
                chunks+=1
            await send(msg)
            if msg['type']=='http.response.body':
                emit(source,'asgi_send_end',request_id=request_id,
                     body_chunk_index=chunks-1,stream_offset=response_total,
                     more_body=msg.get('more_body',False),**common)
        try:await self.app(scope,tracked_receive,tracked_send)
        except BaseException as exc:
            emit(source,'http_error_or_cancel',error_type=type(exc).__name__,**common)
            raise
        finally:
            emit(source,'http_request_lifecycle_end',request_body_bytes=body_total,
                 response_body_bytes=response_total,response_chunks=chunks,**common)
