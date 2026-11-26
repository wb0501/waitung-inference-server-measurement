#!/usr/bin/env python3
"""PID 1: refuse model start without a live guard; stop child on guard heartbeat loss."""
import argparse,json,os,pathlib,signal,subprocess,sys,time
p=argparse.ArgumentParser();p.add_argument('--run-id',required=True);p.add_argument('command',nargs=argparse.REMAINDER);a=p.parse_args()
command=a.command[1:] if a.command and a.command[0]=='--' else a.command
if not command:raise SystemExit('No child command')
def healthy():
 try:
  s=json.loads(pathlib.Path('/guard/status.json').read_text());age=time.monotonic()-s['monotonic']
  return s['run_id']==a.run_id and s['state']=='armed' and 0<=age<=12
 except Exception:return False
if not healthy():raise SystemExit('Refusing to launch child: guard heartbeat absent, unsafe or stale')
child=subprocess.Popen(command,start_new_session=True)
def terminate(reason,code):
 print(f'GUARD SUPERVISOR: {reason}',flush=True)
 try:os.killpg(child.pid,signal.SIGTERM)
 except ProcessLookupError:pass
 try:child.wait(timeout=2)
 except subprocess.TimeoutExpired:
  try:os.killpg(child.pid,signal.SIGKILL)
  except ProcessLookupError:pass
 raise SystemExit(code)
signal.signal(signal.SIGTERM,lambda *_:terminate('Container received SIGTERM',143))
signal.signal(signal.SIGINT,lambda *_:terminate('Container received SIGINT',130))
while child.poll() is None:
 if not healthy():terminate('Guard heartbeat lost or guard triggered; terminating child',70)
 time.sleep(1)
raise SystemExit(child.returncode)
